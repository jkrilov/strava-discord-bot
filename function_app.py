import os
import logging
from datetime import datetime, timezone
from typing import Optional, Any, Dict

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

        # Remove keys with value None (Table Storage doesn't accept None)
        entity = {k: v for k, v in entity.items() if v is not None}

        # First upsert core activity data
        table.upsert_entity(entity=entity, mode="merge")

        # Post or update to Discord if configured
        try:
            _post_or_edit_discord(table, entity)
        except (requests.RequestException, ValueError, OSError):
            logging.exception("Discord post/edit failed")

        logging.info(
            "Upserted club activity by %s %s: %s (PK=%s RK=%s)",
            firstname,
            lastname,
            entity.get("name"),
            partition_key,
            row_key,
        )
    except (AzureError, ValueError, TypeError, OSError):
        logging.exception("Failed processing club activity")


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


def _build_discord_content(entity: Dict[str, Any]) -> str:
    firstname = entity.get("athlete_firstname") or ""
    lastname = entity.get("athlete_lastname") or ""
    athlete_name = (f"{firstname} {lastname}").strip() or "Someone"

    name = entity.get("name") or entity.get("sport_type") or entity.get("type") or "Workout"
    sport = (entity.get("sport_type") or entity.get("type") or "").strip()

    miles = _meters_to_miles(entity.get("distance"))
    moving_secs_val = entity.get("moving_time")
    mins = _seconds_to_minutes(moving_secs_val)

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

    # Include distance if meaningful
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
        if miles is not None and miles >= 0.1 and isinstance(moving_secs_val, (int, float)) and moving_secs_val > 0:
            if _is_pace_sport(sport):
                secs_per_mile = float(moving_secs_val) / float(miles)
                pace_min = int(secs_per_mile // 60)
                pace_sec = int(round(secs_per_mile % 60))
                if pace_sec == 60:
                    pace_min += 1
                    pace_sec = 0
                lines.append(f"🏁 {pace_min}:{pace_sec:02d} /mi")
    except (TypeError, ValueError):
        pass

    # Always include sport label
    if sport:
        lines.append(f"🏅 {sport}")

    content = "\n".join(lines)
    # Discord limit is 2000 chars; keep buffer for safety
    return content[:1800]


def _post_or_edit_discord(table: TableClient, entity: Dict[str, Any]) -> None:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return

    edit_updates = os.getenv("DISCORD_EDIT_UPDATES", "true").lower() == "true"

    content = _build_discord_content(entity)
    if not content:
        return

    existing_content = entity.get("discord_content")
    message_id = entity.get("discord_message_id")

    if edit_updates and message_id and existing_content:
        # Edit existing message if content changed
        if content == existing_content:
            return
        base = webhook_url.split("?")[0].rstrip("/")
        edit_url = f"{base}/messages/{message_id}"
        resp = requests.patch(edit_url, json={"content": content}, timeout=15)
        if resp.status_code in (200, 204):
            entity["discord_content"] = content
            entity["discord_updated_at"] = datetime.now(timezone.utc)
            table.upsert_entity(entity=entity, mode="merge")
        else:
            logging.warning("Discord edit failed: %s %s", resp.status_code, resp.text[:200])
        return

    # Post new message and capture message id with wait=true
    wait_url = webhook_url + ("&wait=true" if "?" in webhook_url else "?wait=true")
    resp = requests.post(wait_url, json={"content": content}, timeout=15)
    if resp.status_code in (200, 204):
        try:
            body = resp.json() if resp.status_code == 200 else {}
        except ValueError:
            body = {}
        msg_id = body.get("id")
        entity["discord_message_id"] = msg_id
        entity["discord_content"] = content
        entity["discord_posted_at"] = datetime.now(timezone.utc)
        table.upsert_entity(entity=entity, mode="merge")
    else:
        logging.warning("Discord post failed: %s %s", resp.status_code, resp.text[:200])


