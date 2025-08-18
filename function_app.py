import os
import logging
import hashlib
import time
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


def discord_retry_with_backoff(func):
    """Custom retry decorator for Discord rate limits."""
    def wrapper(*args, **kwargs):
        max_retries = 5
        base_delay = 2  # Discord webhook rate limit is typically 5 requests per 2 seconds
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except requests.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    # Check for Retry-After header
                    retry_after = e.response.headers.get('Retry-After')
                    if retry_after:
                        delay = int(retry_after)
                    else:
                        # Exponential backoff: 2, 4, 8, 16, 32 seconds
                        delay = base_delay * (2 ** attempt)
                    
                    logging.warning(f"Discord rate limited, retrying in {delay} seconds", extra={
                        "operation": func.__name__,
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "delay_seconds": delay,
                        "retry_after_header": retry_after
                    })
                    
                    if attempt < max_retries - 1:  # Don't sleep on last attempt
                        time.sleep(delay)
                    else:
                        logging.error("Max retries exceeded for Discord API", extra={
                            "operation": func.__name__,
                            "max_retries": max_retries
                        })
                        raise
                else:
                    # For non-rate-limit errors, fail immediately
                    raise
            except Exception:
                # For non-HTTP errors, fail immediately
                raise
        
        return None
    return wrapper


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer")
def poll_strava_activities(myTimer: func.TimerRequest) -> None:
    """Poll Strava club activities every 5 minutes and post to Discord."""
    start_time = datetime.now(timezone.utc)
    logging.info("Starting Strava club activities poll", extra={
        "operation": "poll_strava_activities",
        "start_time": start_time.isoformat()
    })

    activities_processed = 0
    activities_posted = 0
    activities_updated = 0

    try:
        # Get Strava access token
        access_token = get_strava_token()
        if not access_token:
            logging.error("Failed to get Strava access token", extra={
                "operation": "poll_strava_activities",
                "error_type": "auth_failure"
            })
            return

        # Fetch club activities (first page only)
        activities = fetch_club_activities(access_token)
        if not activities:
            logging.info("No activities found", extra={
                "operation": "poll_strava_activities",
                "activities_count": 0
            })
            return

        logging.info(f"Fetched {len(activities)} activities from Strava", extra={
            "operation": "fetch_club_activities",
            "activities_count": len(activities)
        })

        # Get table client
        table_client = get_table_client()
        if not table_client:
            logging.error("Failed to get table client", extra={
                "operation": "poll_strava_activities",
                "error_type": "table_client_failure"
            })
            return

        # Process each activity
        for i, activity in enumerate(activities):
            result = process_activity(activity, table_client)
            activities_processed += 1
            if result == "posted":
                activities_posted += 1
                # Add small delay after posting to respect Discord rate limits
                if i < len(activities) - 1:  # Don't delay after last activity
                    time.sleep(0.5)
            elif result == "updated":
                activities_updated += 1
                # Add small delay after updating to respect Discord rate limits
                if i < len(activities) - 1:  # Don't delay after last activity
                    time.sleep(0.5)

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        logging.info("Completed Strava club activities poll", extra={
            "operation": "poll_strava_activities",
            "duration_seconds": duration,
            "activities_processed": activities_processed,
            "activities_posted": activities_posted,
            "activities_updated": activities_updated,
            "end_time": end_time.isoformat()
        })

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        logging.error(f"Error in poll_strava_activities: {e}", extra={
            "operation": "poll_strava_activities",
            "error_type": "unexpected_error",
            "duration_seconds": duration,
            "activities_processed": activities_processed,
            "exception": str(e)
        }, exc_info=True)


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
        logging.debug("Using cached Strava access token", extra={
            "operation": "get_strava_token",
            "token_source": "cache"
        })
        return _token_cache["access_token"]

    # Refresh token
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    refresh_token = os.getenv("STRAVA_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        logging.error("Missing Strava OAuth credentials", extra={
            "operation": "get_strava_token",
            "error_type": "missing_credentials",
            "has_client_id": bool(client_id),
            "has_client_secret": bool(client_secret),
            "has_refresh_token": bool(refresh_token)
        })
        return None

    try:
        logging.info("Refreshing Strava access token", extra={
            "operation": "get_strava_token",
            "token_source": "refresh"
        })

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

        logging.info("Successfully refreshed Strava access token", extra={
            "operation": "get_strava_token",
            "expires_at": data["expires_at"]
        })

        return data["access_token"]

    except requests.RequestException as e:
        logging.error(f"Failed to refresh Strava token: {e}", extra={
            "operation": "get_strava_token",
            "error_type": "request_failed",
            "status_code": getattr(e.response, 'status_code', None),
            "exception": str(e)
        }, exc_info=True)
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.RequestException)
)
def fetch_club_activities(access_token: str) -> list:
    """Fetch first page of club activities from Strava."""
    club_id = os.getenv("STRAVA_CLUB_ID")
    if not club_id:
        logging.error("STRAVA_CLUB_ID not set", extra={
            "operation": "fetch_club_activities",
            "error_type": "missing_config"
        })
        return []

    try:
        logging.debug("Fetching club activities from Strava", extra={
            "operation": "fetch_club_activities",
            "club_id": club_id
        })

        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(
            f"https://www.strava.com/api/v3/clubs/{club_id}/activities",
            headers=headers,
            params={"page": 1, "per_page": 20},
            timeout=30
        )
        response.raise_for_status()

        activities = response.json()
        logging.info(f"Successfully fetched {len(activities)} activities", extra={
            "operation": "fetch_club_activities",
            "club_id": club_id,
            "activities_count": len(activities),
            "status_code": response.status_code
        })

        return activities

    except requests.RequestException as e:
        logging.error(f"Failed to fetch club activities: {e}", extra={
            "operation": "fetch_club_activities",
            "club_id": club_id,
            "error_type": "request_failed",
            "status_code": getattr(e.response, 'status_code', None),
            "exception": str(e)
        }, exc_info=True)
        raise


def get_table_client() -> Optional[TableClient]:
    """Get Azure Table Storage client using user-assigned managed identity."""
    try:
        table_service_uri = os.getenv("AzureWebJobsStorage__tableServiceUri")
        table_name = os.getenv("STRAVA_ACTIVITIES_TABLE", "StravaActivities")
        client_id = os.getenv("AzureWebJobsStorage__clientId")

        if not table_service_uri:
            logging.error("AzureWebJobsStorage__tableServiceUri not set", extra={
                "operation": "get_table_client",
                "error_type": "missing_config"
            })
            return None

        if not client_id:
            logging.error("AzureWebJobsStorage__clientId not set", extra={
                "operation": "get_table_client",
                "error_type": "missing_client_id"
            })
            return None

        logging.debug("Creating table client with user-assigned managed identity", extra={
            "operation": "get_table_client",
            "table_service_uri": table_service_uri,
            "table_name": table_name,
            "client_id": client_id
        })

        credential = ManagedIdentityCredential(client_id=client_id)
        service_client = TableServiceClient(
            endpoint=table_service_uri,
            credential=credential
        )

        # Ensure table exists
        service_client.create_table_if_not_exists(table_name=table_name)

        table_client = service_client.get_table_client(table_name=table_name)
        
        logging.info("Successfully created table client", extra={
            "operation": "get_table_client",
            "table_name": table_name
        })

        return table_client

    except Exception as e:
        logging.error(f"Failed to create table client: {e}", extra={
            "operation": "get_table_client",
            "error_type": "creation_failed",
            "exception": str(e)
        }, exc_info=True)
        return None


def create_activity_id(activity: Dict[str, Any]) -> str:
    """Create unique ID from athlete name and activity metrics."""
    athlete = activity.get("athlete", {})
    athlete_name = f"{athlete.get('firstname', '')} {athlete.get('lastname', '')}".strip()

    # Create hash from name + key metrics
    data = (f"{athlete_name}:{activity.get('distance', 0)}:"
            f"{activity.get('moving_time', 0)}:{activity.get('elapsed_time', 0)}")
    return hashlib.md5(data.encode()).hexdigest()


def process_activity(activity: Dict[str, Any], table_client: TableClient) -> str:
    """Process a single activity - check if new/changed and post/update Discord."""
    try:
        activity_id = create_activity_id(activity)
        athlete = activity.get("athlete", {})
        athlete_name = f"{athlete.get('firstname', '')} {athlete.get('lastname', '')}".strip()
        
        # Use athlete name as partition key (athlete ID not available in club activities API)
        partition_key = athlete_name or "unknown_athlete"

        logging.debug("Processing activity", extra={
            "operation": "process_activity",
            "activity_id": activity_id,
            "athlete_name": athlete_name,
            "partition_key": partition_key,
            "activity_name": activity.get("name", ""),
            "sport_type": activity.get("sport_type")
        })

        # Check if activity exists in table
        try:
            existing_entity = table_client.get_entity(
                partition_key=partition_key,
                row_key=activity_id
            )
        except AzureError:
            existing_entity = None

        # Create/update entity
        entity = {
            "PartitionKey": partition_key,
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
                logging.info(f"Posted new activity: {entity['activity_name']}", extra={
                    "operation": "process_activity",
                    "action": "posted",
                    "activity_id": activity_id,
                    "athlete_name": athlete_name,
                    "activity_name": entity["activity_name"],
                    "sport_type": entity["sport_type"],
                    "discord_message_id": message_id
                })
                return "posted"
            else:
                logging.warning("Failed to post to Discord", extra={
                    "operation": "process_activity",
                    "action": "post_failed",
                    "activity_id": activity_id,
                    "athlete_name": athlete_name
                })
                return "post_failed"

        elif should_update:
            # Update existing Discord message
            message_id = existing_entity.get("discord_message_id")
            if message_id and update_discord_message(entity, message_id):
                entity["discord_message_id"] = message_id
                table_client.upsert_entity(entity)
                logging.info(f"Updated activity: {entity['activity_name']}", extra={
                    "operation": "process_activity",
                    "action": "updated",
                    "activity_id": activity_id,
                    "athlete_name": athlete_name,
                    "activity_name": entity["activity_name"],
                    "old_name": existing_entity.get("activity_name"),
                    "discord_message_id": message_id
                })
                return "updated"
            else:
                logging.warning("Failed to update Discord message", extra={
                    "operation": "process_activity",
                    "action": "update_failed",
                    "activity_id": activity_id,
                    "athlete_name": athlete_name,
                    "discord_message_id": message_id
                })
                return "update_failed"

        else:
            logging.debug(f"No changes for activity: {entity['activity_name']}", extra={
                "operation": "process_activity",
                "action": "skipped",
                "activity_id": activity_id,
                "athlete_name": athlete_name,
                "activity_name": entity["activity_name"]
            })
            return "skipped"

    except Exception as e:
        logging.error(f"Error processing activity: {e}", extra={
            "operation": "process_activity",
            "error_type": "processing_failed",
            "activity_id": activity_id if 'activity_id' in locals() else "unknown",
            "athlete_name": athlete_name if 'athlete_name' in locals() else "unknown",
            "exception": str(e)
        }, exc_info=True)
        return "error"


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
    lines = [f"🔥 {athlete_name}", f"🏅 {activity_name}"]

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
        lines.append(f"⏱️ Total time: {format_time(moving_time)}")

    # Add separator line to help Discord display messages separately
    lines.append("----------")

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
        return f"📏 Distance: {yards:.0f} yds"
    else:
        # Convert meters to miles
        miles = distance_meters * 0.000621371
        return f"📏 Distance: {miles:.2f} mi"


def format_pace(distance_meters: float, moving_time_seconds: int, sport_type: str) -> str:
    """Format pace based on sport type."""
    if sport_type == "Swim":
        # Time per 100 yards
        yards = distance_meters * 1.094
        if yards > 0:
            seconds_per_100_yards = (moving_time_seconds / yards) * 100
            minutes = int(seconds_per_100_yards // 60)
            seconds = int(seconds_per_100_yards % 60)
            return f"⚡ Pace: {minutes}:{seconds:02d}/100yd"

    elif sport_type in ["Run", "TrailRun", "Walk", "Hike"]:
        # Minutes per mile
        miles = distance_meters * 0.000621371
        if miles > 0:
            minutes_per_mile = moving_time_seconds / 60 / miles
            minutes = int(minutes_per_mile)
            seconds = int((minutes_per_mile - minutes) * 60)
            return f"⚡ Pace: {minutes}:{seconds:02d}/mi"

    elif sport_type == "Ride":
        # Show speed in mph
        miles = distance_meters * 0.000621371
        hours = moving_time_seconds / 3600
        if hours > 0:
            mph = miles / hours
            return f"⚡ Speed: {mph:.1f} mph"

    return ""


def format_time(seconds: int) -> str:
    """Format time duration."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    if hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"


@discord_retry_with_backoff
def post_to_discord(entity: Dict[str, Any]) -> Optional[str]:
    """Post new message to Discord and return message ID."""
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.error("DISCORD_WEBHOOK_URL not set", extra={
            "operation": "post_to_discord",
            "error_type": "missing_config"
        })
        return None

    try:
        message_content = format_discord_message(entity)
        athlete_name = f"{entity.get('athlete_firstname', '')} {entity.get('athlete_lastname', '')}".strip()

        logging.debug("Posting to Discord", extra={
            "operation": "post_to_discord",
            "athlete_name": athlete_name,
            "activity_name": entity.get("activity_name"),
            "sport_type": entity.get("sport_type"),
            "message_length": len(message_content)
        })

        response = requests.post(
            webhook_url,
            json={"content": message_content},
            params={"wait": "true"},
            timeout=30
        )
        response.raise_for_status()

        result = response.json()
        message_id = result.get("id")

        logging.info("Successfully posted to Discord", extra={
            "operation": "post_to_discord",
            "athlete_name": athlete_name,
            "activity_name": entity.get("activity_name"),
            "discord_message_id": message_id,
            "status_code": response.status_code
        })

        return message_id

    except requests.RequestException as e:
        logging.error(f"Failed to post to Discord: {e}", extra={
            "operation": "post_to_discord",
            "error_type": "request_failed",
            "athlete_name": athlete_name if 'athlete_name' in locals() else "unknown",
            "status_code": getattr(e.response, 'status_code', None),
            "exception": str(e)
        }, exc_info=True)
        raise


@discord_retry_with_backoff
def update_discord_message(entity: Dict[str, Any], message_id: str) -> bool:
    """Update existing Discord message."""
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.error("DISCORD_WEBHOOK_URL not set", extra={
            "operation": "update_discord_message",
            "error_type": "missing_config"
        })
        return False

    try:
        # Remove query params and add message ID
        base_url = webhook_url.split("?")[0]
        edit_url = f"{base_url}/messages/{message_id}"

        message_content = format_discord_message(entity)
        athlete_name = f"{entity.get('athlete_firstname', '')} {entity.get('athlete_lastname', '')}".strip()

        logging.debug("Updating Discord message", extra={
            "operation": "update_discord_message",
            "athlete_name": athlete_name,
            "activity_name": entity.get("activity_name"),
            "discord_message_id": message_id,
            "message_length": len(message_content)
        })

        response = requests.patch(
            edit_url,
            json={"content": message_content},
            timeout=30
        )
        response.raise_for_status()

        logging.info("Successfully updated Discord message", extra={
            "operation": "update_discord_message",
            "athlete_name": athlete_name,
            "activity_name": entity.get("activity_name"),
            "discord_message_id": message_id,
            "status_code": response.status_code
        })

        return True

    except requests.RequestException as e:
        logging.error(f"Failed to update Discord message: {e}", extra={
            "operation": "update_discord_message",
            "error_type": "request_failed",
            "athlete_name": athlete_name if 'athlete_name' in locals() else "unknown",
            "discord_message_id": message_id,
            "status_code": getattr(e.response, 'status_code', None),
            "exception": str(e)
        }, exc_info=True)
        raise
