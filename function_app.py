import os
import logging
from datetime import datetime, timezone
from typing import Optional

import azure.functions as func
import requests
from azure.identity import ManagedIdentityCredential
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import AzureError

app = func.FunctionApp()


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
    token = os.getenv("STRAVA_ACCESS_TOKEN")
    since_window = int(os.getenv("STRAVA_SINCE_SECONDS", "3600"))
    if not club_id or not token:
        logging.error("Missing STRAVA_CLUB_ID or STRAVA_ACCESS_TOKEN; skipping run")
        return

    headers = {"Authorization": f"Bearer {token}"}
    # Strava club activities endpoint
    url = f"https://www.strava.com/api/v3/clubs/{club_id}/activities"

    # Optional since filtering: Strava supports 'after' (unix timestamp) for some endpoints; for clubs
    # endpoint, we'll page and filter client-side on 'start_date' or 'updated_at' if present.
    after_ts = int((utc_now.timestamp()) - since_window)
    params = {"per_page": 50, "page": 1}

    collected = 0
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
                if updated:
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
            # Next page
            params["page"] += 1
            # Safety: avoid chasing too far if within small window
            if params["page"] > 10:
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
            except Exception:
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

        table.upsert_entity(entity=entity, mode="merge")

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
    except OSError:
        logging.exception("Failed to create TableClient for activities (OS error)")
        return None


