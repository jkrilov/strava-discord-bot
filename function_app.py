import os
import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import azure.functions as func
import requests
from azure.identity import ManagedIdentityCredential
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import AzureError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

app = func.FunctionApp()

# Strava OAuth token cache
_token_cache = {"access_token": None, "expires_at": 0}


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer")
def poll_strava_activities(myTimer: func.TimerRequest) -> None:
    """Poll Strava club activities every 5 minutes and post to Discord."""
    logging.info("Starting Strava club activities poll")

    try:
        # Get Strava access token
        access_token = get_strava_token()
        if not access_token:
            logging.error("Failed to get Strava access token")
            return

        # Fetch club activities (first page only)
        activities = fetch_club_activities(access_token)
        if not activities:
            logging.info("No activities found")
            return

        # Get table client
        table_client = get_table_client()
        if not table_client:
            logging.error("Failed to get table client")
            return

        # Process each activity
        for activity in activities:
            process_activity(activity, table_client)

        logging.info(f"Processed {len(activities)} activities")

    except Exception as e:
        logging.error(f"Error in poll_strava_activities: {e}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.RequestException, AzureError))
)
def get_strava_token() -> Optional[str]:
    """Get Strava access token using refresh token."""
    global _token_cache

    # Check if cached token is still valid (with 5 minute buffer)
    now = int(datetime.now(timezone.utc).timestamp())
    if (_token_cache["access_token"] and
            _token_cache["expires_at"] > now + 300):
        return _token_cache["access_token"]

    # Refresh token
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        logging.error("Missing Strava OAuth credentials")
        return None

    response = requests.post(
        "https://www.strava.com/api/v3/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token"
        },
        timeout=30
    )
    response.raise_for_status()

    data = response.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = data["expires_at"]

    return data["access_token"]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.RequestException)
)
def fetch_club_activities(access_token: str) -> list:
    """Fetch first page of club activities from Strava."""
    club_id = os.getenv("STRAVA_CLUB_ID")
    if not club_id:
        logging.error("STRAVA_CLUB_ID not set")
        return []

    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(
        f"https://www.strava.com/api/v3/clubs/{club_id}/activities",
        headers=headers,
        params={"page": 1, "per_page": 30},
        timeout=30
    )
    response.raise_for_status()

    return response.json()


def get_table_client() -> Optional[TableClient]:
    """Get Azure Table Storage client using managed identity."""
    try:
        table_service_uri = os.getenv("AzureWebJobsStorage__tableServiceUri")
        table_name = os.getenv("STRAVA_ACTIVITIES_TABLE", "StravaActivities")

        if not table_service_uri:
            logging.error("AzureWebJobsStorage__tableServiceUri not set")
            return None

        credential = ManagedIdentityCredential()
        service_client = TableServiceClient(
            endpoint=table_service_uri,
            credential=credential
        )

        # Ensure table exists
        service_client.create_table_if_not_exists(table_name=table_name)

        return service_client.get_table_client(table_name=table_name)

    except Exception as e:
        logging.error(f"Failed to create table client: {e}")
        return None


def create_activity_id(activity: Dict[str, Any]) -> str:
    """Create unique ID from athlete name and activity metrics."""
    athlete = activity.get("athlete", {})
    athlete_name = f"{athlete.get('firstname', '')} {athlete.get('lastname', '')}".strip()

    # Create hash from name + key metrics
    data = (f"{athlete_name}:{activity.get('distance', 0)}:"
            f"{activity.get('moving_time', 0)}:{activity.get('elapsed_time', 0)}")
    return hashlib.md5(data.encode()).hexdigest()


def process_activity(activity: Dict[str, Any], table_client: TableClient) -> None:
    """Process a single activity - check if new/changed and post/update Discord."""
    try:
        activity_id = create_activity_id(activity)
        athlete = activity.get("athlete", {})
        athlete_id = athlete.get("id", "unknown")

        # Check if activity exists in table
        try:
            existing_entity = table_client.get_entity(
                partition_key=str(athlete_id),
                row_key=activity_id
            )
        except AzureError:
            existing_entity = None

        # Create/update entity
        entity = {
            "PartitionKey": str(athlete_id),
            "RowKey": activity_id,
            "activity_name": activity.get("name", ""),
            "athlete_firstname": athlete.get("firstname", ""),
            "athlete_lastname": athlete.get("lastname", ""),
            "distance": activity.get("distance"),
            "moving_time": activity.get("moving_time"),
            "elapsed_time": activity.get("elapsed_time"),
            "total_elevation_gain": activity.get("total_elevation_gain"),
            "sport_type": activity.get("sport_type"),
            "workout_type": activity.get("workout_type"),
            "last_updated": datetime.now(timezone.utc)
        }

        # Determine if we need to post or update
        should_post = existing_entity is None
        should_update = (existing_entity is not None and
                         existing_entity.get("activity_name") != entity["activity_name"])

        if should_post:
            # Post new message to Discord
            message_id = post_to_discord(entity)
            if message_id:
                entity["discord_message_id"] = message_id
                table_client.upsert_entity(entity)
                logging.info(f"Posted new activity: {entity['activity_name']}")

        elif should_update:
            # Update existing Discord message
            message_id = existing_entity.get("discord_message_id")
            if message_id and update_discord_message(entity, message_id):
                entity["discord_message_id"] = message_id
                table_client.upsert_entity(entity)
                logging.info(f"Updated activity: {entity['activity_name']}")

        else:
            logging.debug(f"No changes for activity: {entity['activity_name']}")

    except Exception as e:
        logging.error(f"Error processing activity: {e}")


def format_discord_message(entity: Dict[str, Any]) -> str:
    """Format activity data for Discord message."""
    firstname = entity.get("athlete_firstname", "")
    lastname = entity.get("athlete_lastname", "")
    athlete_name = f"{firstname} {lastname}".strip() or "Unknown Athlete"

    activity_name = entity.get("activity_name", "Activity")
    sport_type = entity.get("sport_type", "")
    distance = entity.get("distance")
    moving_time = entity.get("moving_time")

    # Start building message
    lines = [f"🏃 {athlete_name}", f"📝 {activity_name}"]

    # Add sport type emoji
    sport_emoji = get_sport_emoji(sport_type)
    if sport_emoji:
        lines.append(f"{sport_emoji} {sport_type}")

    # Add distance (skip for certain activity types)
    if should_include_distance(sport_type) and distance:
        distance_text = format_distance(distance, sport_type)
        if distance_text:
            lines.append(distance_text)

    # Add pace/speed if we have distance and time
    if distance and moving_time and should_include_distance(sport_type):
        pace_text = format_pace(distance, moving_time, sport_type)
        if pace_text:
            lines.append(pace_text)

    # Add moving time
    if moving_time:
        lines.append(f"⏱️ {format_time(moving_time)}")

    return "\n".join(lines)


def get_sport_emoji(sport_type: str) -> str:
    """Get emoji for sport type."""
    emoji_map = {
        "Run": "🏃",
        "TrailRun": "🥾",
        "Ride": "🚴",
        "Swim": "🏊",
        "Walk": "🚶",
        "Hike": "🥾",
        "Workout": "💪",
        "WeightTraining": "🏋️",
        "Yoga": "🧘",
        "Rowing": "🚣"
    }
    return emoji_map.get(sport_type, "🏅")


def should_include_distance(sport_type: str) -> bool:
    """Check if distance should be included for this sport type."""
    no_distance_sports = {"Workout", "WeightTraining", "Yoga", "Meditation"}
    return sport_type not in no_distance_sports


def format_distance(distance_meters: float, sport_type: str) -> str:
    """Format distance with appropriate units."""
    if sport_type == "Swim":
        # Convert meters to yards
        yards = distance_meters * 1.094
        return f"📏 {yards:.0f} yds"
    else:
        # Convert meters to miles
        miles = distance_meters * 0.000621371
        return f"📏 {miles:.2f} mi"


def format_pace(distance_meters: float, moving_time_seconds: int, sport_type: str) -> str:
    """Format pace based on sport type."""
    if sport_type == "Swim":
        # Time per 100 yards
        yards = distance_meters * 1.094
        if yards > 0:
            seconds_per_100_yards = (moving_time_seconds / yards) * 100
            minutes = int(seconds_per_100_yards // 60)
            seconds = int(seconds_per_100_yards % 60)
            return f"⚡ {minutes}:{seconds:02d}/100yd"

    elif sport_type in ["Run", "TrailRun", "Walk", "Hike"]:
        # Minutes per mile
        miles = distance_meters * 0.000621371
        if miles > 0:
            minutes_per_mile = moving_time_seconds / 60 / miles
            minutes = int(minutes_per_mile)
            seconds = int((minutes_per_mile - minutes) * 60)
            return f"⚡ {minutes}:{seconds:02d}/mi"

    elif sport_type == "Ride":
        # Show speed in mph
        miles = distance_meters * 0.000621371
        hours = moving_time_seconds / 3600
        if hours > 0:
            mph = miles / hours
            return f"⚡ {mph:.1f} mph"

    return ""


def format_time(seconds: int) -> str:
    """Format time duration."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    if hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.RequestException)
)
def post_to_discord(entity: Dict[str, Any]) -> Optional[str]:
    """Post new message to Discord and return message ID."""
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.error("DISCORD_WEBHOOK_URL not set")
        return None

    message_content = format_discord_message(entity)

    response = requests.post(
        webhook_url,
        json={"content": message_content},
        params={"wait": "true"},
        timeout=30
    )
    response.raise_for_status()

    return response.json().get("id")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.RequestException)
)
def update_discord_message(entity: Dict[str, Any], message_id: str) -> bool:
    """Update existing Discord message."""
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.error("DISCORD_WEBHOOK_URL not set")
        return False

    # Remove query params and add message ID
    base_url = webhook_url.split("?")[0]
    edit_url = f"{base_url}/messages/{message_id}"

    message_content = format_discord_message(entity)

    response = requests.patch(
        edit_url,
        json={"content": message_content},
        timeout=30
    )
    response.raise_for_status()

    return True
