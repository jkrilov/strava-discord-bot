import os
import logging
from datetime import datetime, timezone
from typing import Optional, Any, Dict
from datetime import timedelta

import azure.functions as func
import requests
from azure.identity import ManagedIdentityCredential
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import AzureError

app = func.FunctionApp()

# Simple in-memory token cache for refresh-token flow
_TOKEN_CACHE: Dict[str, Any] = {
    "access_token": None,
    "expires_at": 0,
    "refresh_token": None,
}

# Per-run counter for Discord posts to optionally add separators
_RUN_POST_COUNT: int = 0


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer")
def collect_club_activities(myTimer: func.TimerRequest) -> None:
    """Timer runs every 5 minutes to collect latest activities for a Strava club.

    Env vars required:
    - STRAVA_CLUB_ID: The Strava club ID to poll
    - STRAVA_ACCESS_TOKEN: A personal access token or app token authorized to read club activities
      (From a Strava account with access to the club)
    - STRAVA_SINCE_SECONDS: Optional; only fetch activities updated in the last N seconds (default: 3600)
    """
    utc_now = datetime.now(timezone.utc)
    # Reset per-run counter for Discord posts
    global _RUN_POST_COUNT
    _RUN_POST_COUNT = 0
    logging.info("collect_club_activities invoked at %s", utc_now.isoformat())

    club_id = os.getenv("STRAVA_CLUB_ID")
    token = _get_bearer_token()
    since_window = int(os.getenv("STRAVA_SINCE_SECONDS", "3600"))
    per_page = int(os.getenv("STRAVA_PER_PAGE", "50") or "50")
    max_pages = int(os.getenv("STRAVA_MAX_PAGES", "10") or "10")
    max_items = int(os.getenv("STRAVA_MAX_ACTIVITIES", "0") or "0")  # 0 = unlimited
    use_time_filter = (os.getenv("STRAVA_USE_TIME_FILTER", "true").lower() == "true")
    if not club_id or not token:
        logging.error(
            "Missing STRAVA_CLUB_ID or token; set STRAVA_ACCESS_TOKEN or "
            "STRAVA_CLIENT_ID/STRAVA_CLIENT_SECRET/STRAVA_REFRESH_TOKEN",
        )
        return

    headers = {"Authorization": f"Bearer {token}"}
    # Strava club activities endpoint
    url = f"https://www.strava.com/api/v3/clubs/{club_id}/activities"

    # Optional since filtering: Strava supports 'after' (unix timestamp) for some endpoints; for clubs
    # endpoint, we'll page and filter client-side on 'start_date' or 'updated_at' if present.
    after_ts = int((utc_now.timestamp()) - since_window)
    params = {"per_page": per_page, "page": 1}

    collected = 0
    done = False
    try:
        while True:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            if resp.status_code != 200:
                logging.warning("Strava clubs activities failed: status=%s body=%s", resp.status_code, resp.text[:200])
                break
            items = resp.json() or []
            if not items:
                break
            for act in items:
                # Filter by time window if possible
                updated = act.get("updated_at") or act.get("start_date")
                keep = True
                if use_time_filter and updated:
                    try:
                        # updated_at/start_date are ISO-8601; compare to after_ts
                        ts = int(datetime.fromisoformat(updated.replace("Z", "+00:00")).timestamp())
                        keep = ts >= after_ts
                    except (ValueError, TypeError, OverflowError):
                        keep = True
                if not keep:
                    continue
                collected += 1
                _process_club_activity(act)
                if max_items > 0 and collected >= max_items:
                    done = True
                    break
            # Next page
            params["page"] += 1
            # Safety: avoid chasing too far if within small window
            if params["page"] > max_pages:
                break
            if done:
                break
    except requests.RequestException:
        logging.exception("Network error fetching club activities")

    logging.info("Collected %s activities for club %s", collected, club_id)


def _process_club_activity(activity: dict) -> None:
    """Handle a single club activity record.

    Stores a flattened record into Azure Table Storage using a synthetic RowKey
    because club activities do not include the activity ID.
    """
    try:
        athlete = activity.get("athlete", {}) or {}
        firstname = (athlete.get("firstname") or "").strip()
        lastname = (athlete.get("lastname") or "").strip()
        athlete_id = athlete.get("id")

        # Build synthetic ID from requested fields; normalize floats to integers (meters) to avoid
        # floating-point representation differences.
        distance = activity.get("distance")
        moving_time = activity.get("moving_time")
        elapsed_time = activity.get("elapsed_time")
        total_elevation_gain = activity.get("total_elevation_gain")

        # Normalize numeric values for ID construction
        def _n_int(val: object) -> int:
            if val is None:
                return 0
            if isinstance(val, (int, float)):
                return int(round(float(val)))
            if isinstance(val, str) and val.strip():
                try:
                    return int(round(float(val)))
                except ValueError:
                    return 0
            return 0

        synthetic_id = (
            f"{firstname}:{_n_int(distance)}:{_n_int(moving_time)}:"
            f"{_n_int(elapsed_time)}:{_n_int(total_elevation_gain)}"
        )

        # Choose partition key: prefer athlete_id, else athlete name
        partition_key = str(athlete_id) if athlete_id is not None else (firstname or "unknown")
        row_key = synthetic_id

        table = _get_activities_table_client()
        if table is None:
            logging.warning(
                "Table client not available; skipping persistence for activity by %s %s",
                firstname,
                lastname,
            )
            return

        # Flatten entity for Table Storage
        entity = {
            "PartitionKey": partition_key,
            "RowKey": row_key,
            # Core fields from the club activity payload
            "name": activity.get("name"),
            "distance": float(distance) if distance is not None else None,
            "moving_time": int(moving_time) if moving_time is not None else None,
            "elapsed_time": int(elapsed_time) if elapsed_time is not None else None,
            "total_elevation_gain": float(total_elevation_gain) if total_elevation_gain is not None else None,
            "type": activity.get("type"),  # deprecated but present
            "sport_type": activity.get("sport_type"),
            "workout_type": activity.get("workout_type"),
            # Timestamps from Strava payload (strings)
            "start_date": activity.get("start_date"),
            "start_date_local": activity.get("start_date_local"),
            "strava_updated_at": activity.get("updated_at"),
            "club_activity_id": activity.get("id"),  # if Strava includes one
            "club_id": os.getenv("STRAVA_CLUB_ID"),
            # Flattened athlete
            "athlete_id": athlete_id if athlete_id is not None else None,
            "athlete_firstname": firstname or None,
            "athlete_lastname": lastname or None,
            "athlete_json": None,
            # Bookkeeping
            "source": "club",
            "updated_at": datetime.now(timezone.utc),
        }

        # Load existing entity to preserve Discord fields for dedupe/edit logic
        try:
            existing = table.get_entity(partition_key=partition_key, row_key=row_key)
        except AzureError:
            existing = None
        except Exception:
            existing = None

        if isinstance(existing, dict):
            for k in (
                "discord_message_id",
                "discord_content",
                "discord_posted_at",
                "discord_updated_at",
                "created_at",
            ):
                if existing.get(k) is not None and entity.get(k) is None:
                    entity[k] = existing.get(k)

        # Include athlete JSON if present
        if athlete:
            try:
                import json as _json  # local import to avoid top-level dependency at cold start cost

                entity["athlete_json"] = _json.dumps(athlete, ensure_ascii=False)
            except (TypeError, ValueError):
                # best-effort only
                pass

        # Copy any additional simple scalar fields from the payload
        for k, v in activity.items():
            if k in entity or k == "athlete":
                continue
            if isinstance(v, (str, int, float, bool)) and len(k) < 255:
                # Avoid overwriting our bookkeeping names
                safe_key = k if k not in {"PartitionKey", "RowKey"} else f"strava_{k}"
                entity.setdefault(safe_key, v)

        # Initialize created_at if missing
        if entity.get("created_at") is None:
            entity["created_at"] = datetime.now(timezone.utc)

        # Remove keys with value None (Table Storage doesn't accept None)
        entity = {k: v for k, v in entity.items() if v is not None}

        # First upsert core activity data
        table.upsert_entity(entity=entity, mode="merge")

        # Post or update to Discord if configured
        try:
            _post_or_edit_discord(table, entity)
        except (requests.RequestException, ValueError, OSError):
            logging.exception("Discord post/edit failed")

        logging.debug(
            "Upserted club activity by %s %s: %s (PK=%s RK=%s)",
            firstname,
            lastname,
            entity.get("name"),
            partition_key,
            row_key,
        )
    except (AzureError, ValueError, TypeError, OSError):
        logging.exception("Failed processing club activity")


def _to_utc_iso(dt: datetime) -> str:
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Compaction removed: storage is minimal; we simply delete old entities in cleanup.


@app.timer_trigger(schedule="0 0 4 * * *", arg_name="cleanupTimer")
def cleanup_old_entities(cleanupTimer: func.TimerRequest) -> None:
    """Daily cleanup: delete activities older than retention.

    Env:
    - STRAVA_DELETE_AFTER_DAYS: delete entities older than this (default 180)
    - STRAVA_CLEANUP_MAX: max entities to process per run (default 500)
    """
    try:
        table = _get_activities_table_client()
        if table is None:
            return

        delete_days = int(os.getenv("STRAVA_DELETE_AFTER_DAYS", "180") or "180")
        max_ops = int(os.getenv("STRAVA_CLEANUP_MAX", "500") or "500")
        if delete_days <= 0:
            return

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=delete_days)
        cutoff_iso = _to_utc_iso(cutoff)

        processed = 0
        # Prefer created_at; fallback to updated_at if created_at missing
        filter_created = f"created_at lt datetime'{cutoff_iso}'"
        filter_updated = f"updated_at lt datetime'{cutoff_iso}' and (created_at eq null)"

        try:
            # Delete by created_at first
            for page in table.list_entities(results_per_page=1000, query_filter=filter_created).by_page():
                for e in page:
                    try:
                        pk = e.get("PartitionKey")
                        rk = e.get("RowKey")
                        if not pk or not rk:
                            continue
                        table.delete_entity(partition_key=pk, row_key=rk)
                        processed += 1
                        if processed >= max_ops:
                            raise StopIteration
                    except StopIteration:
                        raise
                    except AzureError:
                        logging.exception(
                            "Cleanup: failed to delete entity PK=%s RK=%s (created_at)",
                            e.get("PartitionKey"),
                            e.get("RowKey"),
                        )
                    except Exception:
                        logging.exception("Cleanup: unexpected error during deletion (created_at)")
        except StopIteration:
            pass
        except AzureError:
            logging.exception("Cleanup: query for deletion by created_at failed")

        # If capacity remains, delete by updated_at for legacy rows without created_at
        if processed < max_ops:
            remaining = max_ops - processed
            try:
                for page in table.list_entities(results_per_page=1000, query_filter=filter_updated).by_page():
                    for e in page:
                        try:
                            pk = e.get("PartitionKey")
                            rk = e.get("RowKey")
                            if not pk or not rk:
                                continue
                            table.delete_entity(partition_key=pk, row_key=rk)
                            processed += 1
                            if processed >= max_ops or processed >= remaining:
                                raise StopIteration
                        except StopIteration:
                            raise
                        except AzureError:
                            logging.exception(
                                "Cleanup: failed to delete entity PK=%s RK=%s (updated_at)",
                                e.get("PartitionKey"),
                                e.get("RowKey"),
                            )
                        except Exception:
                            logging.exception("Cleanup: unexpected error during deletion (updated_at)")
            except StopIteration:
                pass
            except AzureError:
                logging.exception("Cleanup: query for deletion by updated_at failed")

        logging.info("Cleanup completed: deleted=%s", processed)
    except Exception:
        logging.exception("Cleanup: fatal error")


def _get_activities_table_client() -> Optional[TableClient]:
    """Create or return a TableClient for the StravaActivities table using Managed Identity.

    Expects:
    - AzureWebJobsStorage__tableServiceUri
    - Optional: AzureWebJobsStorage__clientId (for user-assigned managed identity)
    - Optional: STRAVA_ACTIVITIES_TABLE (defaults to 'StravaActivities')
    """
    try:
        table_service_uri = os.getenv("AzureWebJobsStorage__tableServiceUri")
        if not table_service_uri:
            logging.error("AzureWebJobsStorage__tableServiceUri not set; cannot access Table Storage")
            return None

        client_id = (
            os.getenv("AzureWebJobsStorage__clientId")
            or os.getenv("USER_ASSIGNED_MANAGED_IDENTITY_CLIENT_ID")
            or os.getenv("AZURE_CLIENT_ID")
        )
        credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()

        service = TableServiceClient(endpoint=table_service_uri, credential=credential)
        table_name = os.getenv("STRAVA_ACTIVITIES_TABLE", "StravaActivities")
        # Ensure table exists
        try:
            service.create_table_if_not_exists(table_name=table_name)
        except AzureError:
            # table may already exist or lack permission to create
            pass
        return service.get_table_client(table_name=table_name)
    except AzureError:
        logging.exception("Failed to create TableClient for activities (Azure error)")
        return None


def _get_bearer_token() -> Optional[str]:
    """Return a Strava bearer token.

    Preference order:
    1) STRAVA_ACCESS_TOKEN (direct)
    2) Refresh-token flow using STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN
    """
    direct = os.getenv("STRAVA_ACCESS_TOKEN")
    if direct:
        return direct

    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")
    if not (client_id and client_secret and refresh_token):
        return None

    # Use cache if still valid for at least 60 seconds
    now = int(datetime.now(timezone.utc).timestamp())
    try:
        cached_token = _TOKEN_CACHE.get("access_token")
        exp_val: Any = _TOKEN_CACHE.get("expires_at")
        if isinstance(exp_val, (int, float, str)):
            try:
                cached_exp = int(exp_val)
            except (TypeError, ValueError):
                cached_exp = 0
        else:
            cached_exp = 0
        cached_rt = _TOKEN_CACHE.get("refresh_token")
    except (KeyError, TypeError):
        cached_token = None
        cached_exp = 0
        cached_rt = None

    if cached_token and cached_exp - now > 60 and cached_rt == refresh_token:
        return str(cached_token)

    # Refresh via Strava OAuth endpoint
    try:
        resp = requests.post(
            "https://www.strava.com/api/v3/oauth/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logging.warning("Strava token refresh failed: %s %s", resp.status_code, resp.text[:200])
            return None
        body = resp.json() or {}
        access_token = body.get("access_token")
        expires_at = int(body.get("expires_at") or 0)
        new_refresh = body.get("refresh_token") or refresh_token
        if not access_token:
            return None
        _TOKEN_CACHE["access_token"] = access_token
        _TOKEN_CACHE["expires_at"] = expires_at
        _TOKEN_CACHE["refresh_token"] = new_refresh
        return access_token
    except requests.RequestException:
        logging.exception("Failed to refresh Strava access token")
        return None
    except OSError:
        logging.exception("Failed to create TableClient for activities (OS error)")
        return None


def _meters_to_miles(meters: Any) -> Optional[float]:
    try:
        if meters is None:
            return None
        m = float(meters)
        return m / 1609.344
    except (TypeError, ValueError):
        return None


def _seconds_to_minutes(seconds: Any) -> Optional[int]:
    try:
        if seconds is None:
            return None
        s = float(seconds)
        return int(round(s / 60.0))
    except (TypeError, ValueError):
        return None


def _build_discord_content(entity: Dict[str, Any], include_separator: bool = False) -> str:
    firstname = entity.get("athlete_firstname") or ""
    lastname = entity.get("athlete_lastname") or ""
    athlete_name = (f"{firstname} {lastname}").strip() or "Someone"

    name = entity.get("name") or entity.get("sport_type") or entity.get("type") or "Workout"
    sport = (entity.get("sport_type") or entity.get("type") or "").strip()

    distance_m = entity.get("distance")
    miles = _meters_to_miles(distance_m)
    moving_secs_val = entity.get("moving_time")
    mins = _seconds_to_minutes(moving_secs_val)

    def workout_type_label(sport_name: str, wt: Any) -> Optional[str]:
        # Normalize
        try:
            code = int(wt) if wt is not None and str(wt).strip() != "" else None
        except (TypeError, ValueError):
            code = None
        if code is None:
            return None
        s = sport_name or ""
        is_run = s in ("Run", "TrailRun")
        is_ride = "Ride" in s and s not in ("Run", "TrailRun")
        if is_run:
            return {1: "Race", 2: "Long Run", 3: "Workout"}.get(code)
        if is_ride:
            return {10: "Race", 11: "Workout"}.get(code)
        return None

    def sport_emoji(s: str) -> str:
        m = {
            "Run": "🏃",
            "Ride": "🚴",
            "Walk": "🚶",
            "Hike": "🥾",
            "Swim": "🏊",
            "Workout": "🏋️",
            "WeightTraining": "🏋️",
            "Yoga": "🧘",
            "Rowing": "🚣",
        }
        return m.get(s, "🏅") if s else "🏅"

    lines: list[str] = []
    lines.append(f"🔥 {athlete_name}")
    lines.append(f"{sport_emoji(sport)} {name}")

    # Include distance: swims stay in meters; others in miles
    is_swim = "Swim" in sport
    is_ride = "Ride" in sport
    if is_swim:
        try:
            if distance_m is not None and float(distance_m) > 0:
                lines.append(f"📏 {int(round(float(distance_m)))} m")
        except (TypeError, ValueError):
            pass
    else:
        if miles is not None and miles >= 0.05:
            lines.append(f"📏 {miles:.2f} mi")

    # Include moving time when available
    if mins is not None and mins > 0:
        lines.append(f"⏱️ {mins} min")

    # Add pace for relevant sports when distance/time are meaningful
    def _is_pace_sport(s: str) -> bool:
        s = s or ""
        return any(x in s for x in ["Run", "Walk", "Hike"])  # Run/TrailRun/Walk/Hike

    try:
        if isinstance(moving_secs_val, (int, float)) and moving_secs_val > 0:
            if is_swim and distance_m is not None and float(distance_m) > 0:
                # Pace per 100m
                secs_per_100m = float(moving_secs_val) / (float(distance_m) / 100.0)
                pace_min = int(secs_per_100m // 60)
                pace_sec = int(round(secs_per_100m % 60))
                if pace_sec == 60:
                    pace_min += 1
                    pace_sec = 0
                lines.append(f"🏁 {pace_min}:{pace_sec:02d} /100m")
            elif miles is not None and miles >= 0.1 and _is_pace_sport(sport):
                # Pace for run/walk/hike
                secs_per_mile = float(moving_secs_val) / float(miles)
                pace_min = int(secs_per_mile // 60)
                pace_sec = int(round(secs_per_mile % 60))
                if pace_sec == 60:
                    pace_min += 1
                    pace_sec = 0
                lines.append(f"🏁 {pace_min}:{pace_sec:02d} /mi")
            elif is_ride:
                # Speed for rides (mph)
                mph = None
                if miles is not None and miles > 0.05:
                    hours = float(moving_secs_val) / 3600.0
                    if hours > 0:
                        mph = float(miles) / hours
                if mph is None:
                    avg_mps = entity.get("average_speed")
                    try:
                        if avg_mps is not None:
                            mph = float(avg_mps) * 2.2369362920544
                    except (TypeError, ValueError):
                        mph = None
                if mph is not None and mph > 0.5:
                    lines.append(f"⚡ {mph:.1f} mph")
    except (TypeError, ValueError):
        pass

    # Always include sport label, and append workout type mapping when available
    if sport:
        wt_label = workout_type_label(sport, entity.get("workout_type"))
        if wt_label:
            lines.append(f"🏅 {sport} - {wt_label}")
        else:
            lines.append(f"🏅 {sport}")

    # Optional separator for multi-activity runs
    try:
        add_sep = os.getenv("DISCORD_ADD_SEPARATOR", "true").lower() == "true"
    except Exception:
        add_sep = True
    if include_separator and add_sep:
        try:
            # Append a separator starting from the second post
            if _RUN_POST_COUNT >= 1:
                lines.append("────────")
        except NameError:
            # Counter not initialized; ignore
            pass

    content = "\n".join(lines)
    # Discord limit is 2000 chars; keep buffer for safety
    return content[:1800]


def _post_or_edit_discord(table: TableClient, entity: Dict[str, Any]) -> None:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return

    edit_updates = os.getenv("DISCORD_EDIT_UPDATES", "true").lower() == "true"

    # Build a base content used for storage and comparisons (no per-run separator)
    base_content = _build_discord_content(entity, include_separator=False)
    if not base_content:
        return

    existing_content = entity.get("discord_content")
    message_id = entity.get("discord_message_id")

    # If a message already exists and edits are disabled, skip to prevent duplicates
    if message_id and not edit_updates:
        return

    if edit_updates and message_id and existing_content is not None:
        # Edit existing message if content changed
        if base_content == existing_content:
            return
        # Log a concise diff to diagnose why we edit
        try:
            old_lines = str(existing_content).splitlines()
            new_lines = str(base_content).splitlines()
            first_diff_idx = None
            for i in range(min(len(old_lines), len(new_lines))):
                if old_lines[i] != new_lines[i]:
                    first_diff_idx = i
                    break
            if first_diff_idx is None and len(old_lines) != len(new_lines):
                first_diff_idx = min(len(old_lines), len(new_lines))
            if first_diff_idx is not None:
                logging.info(
                    "Discord edit: first diff at line %s | old='%s' | new='%s'",
                    first_diff_idx,
                    (old_lines[first_diff_idx] if first_diff_idx < len(old_lines) else "<no line>")[:160],
                    (new_lines[first_diff_idx] if first_diff_idx < len(new_lines) else "<no line>")[:160],
                )
            else:
                logging.info("Discord edit: content changed (no line diff found, possibly whitespace)")
        except Exception:
            pass
        base = webhook_url.split("?")[0].rstrip("/")
        edit_url = f"{base}/messages/{message_id}"
        resp = requests.patch(edit_url, json={"content": base_content}, timeout=15)
        if resp.status_code in (200, 204):
            entity["discord_content"] = base_content
            entity["discord_updated_at"] = datetime.now(timezone.utc)
            table.upsert_entity(entity=entity, mode="merge")
        else:
            logging.warning("Discord edit failed: %s %s", resp.status_code, resp.text[:200])
        return

    # Post new message and capture message id with wait=true
    wait_url = webhook_url + ("&wait=true" if "?" in webhook_url else "?wait=true")
    # For new posts we may include a separator visually, but we keep base_content in storage
    posted_content = _build_discord_content(entity, include_separator=True)
    resp = requests.post(wait_url, json={"content": posted_content}, timeout=15)
    if resp.status_code in (200, 204):
        try:
            body = resp.json() if resp.status_code == 200 else {}
        except ValueError:
            body = {}
        msg_id = body.get("id")
        entity["discord_message_id"] = msg_id
        entity["discord_content"] = base_content
        entity["discord_posted_at"] = datetime.now(timezone.utc)
        table.upsert_entity(entity=entity, mode="merge")
        # Increment per-run post counter on successful new post
        try:
            global _RUN_POST_COUNT
            _RUN_POST_COUNT += 1
        except Exception:
            pass
    else:
        logging.warning("Discord post failed: %s %s", resp.status_code, resp.text[:200])


