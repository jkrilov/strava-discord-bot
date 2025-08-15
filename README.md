# Strava → Discord Bot (Timer, Azure Functions, Python)

Azure Functions (Python 3.12) app that polls a Strava club feed on a schedule and stores activities in Azure Table Storage. Designed for cases where webhooks aren’t used.

## Features

- Timer-triggered poll every 5 minutes (cron: `0 */5 * * * *`)
- Fetches latest Strava club activities with your access token
- Synthetic ID (since club feed lacks activity IDs)
- Upserts flattened activity records into Azure Table Storage via Managed Identity
- Ready to add Discord posting after persistence

## How it works

- The timer function calls `GET /api/v3/clubs/{club_id}/activities`.
- Results are filtered to a recent window (default last 3600 seconds) and paged.
- Each item is stored in `StravaActivities` (configurable) using:
  - PartitionKey: `athlete_id` if present, else athlete firstname (fallback `unknown`)
  - RowKey (synthetic): `firstname:distance:moving_time:elapsed_time:total_elevation_gain`
- All scalar fields from the payload are saved; athlete is also stored as JSON.

## Environment variables

Required:
- `STRAVA_CLUB_ID` — Club ID to poll
- `STRAVA_ACCESS_TOKEN` — Token that can read the club feed
- `AzureWebJobsStorage__tableServiceUri` — `https://<storage>.table.core.windows.net`

Optional:
- `STRAVA_SINCE_SECONDS` — Recent window in seconds (default: `3600`)
- `STRAVA_ACTIVITIES_TABLE` — Table name (default: `StravaActivities`)
- `AzureWebJobsStorage__clientId` — Client ID for a user-assigned managed identity

## Managed Identity and RBAC

Grant the Function App’s managed identity the `Storage Table Data Contributor` role on the storage account. If using a user-assigned identity, set `AzureWebJobsStorage__clientId`.

## Data model (Azure Table Storage)

Table: `StravaActivities`
- PK: `athlete_id` (if present) else athlete firstname (`unknown` fallback)
- RK: `firstname:distance:moving_time:elapsed_time:total_elevation_gain`
- Stored columns include:
  - `name`, `distance`, `moving_time`, `elapsed_time`, `total_elevation_gain`
  - `type` (deprecated), `sport_type`, `workout_type`
  - `start_date`, `start_date_local`, `strava_updated_at`
  - `club_activity_id` (if present), `club_id`, `source="club"`, `updated_at` (UTC)
  - `athlete_id`, `athlete_firstname`, `athlete_lastname`, `athlete_json`
- Additional scalar fields from the payload are included when available. `None` values are omitted.

## Local development

- Prefer testing in Azure with MI and proper RBAC.
- Running locally requires `AzureWebJobsStorage__tableServiceUri` and an identity with table access.
- Dependencies: see `requirements.txt`.

## Troubleshooting

- Missing table client: ensure `AzureWebJobsStorage__tableServiceUri` is set and MI has `Storage Table Data Contributor`.
- 403 AuthorizationPermissionMismatch: verify role scope; allow propagation; restart the Function App.
- No activities stored: validate `STRAVA_ACCESS_TOKEN` and `STRAVA_CLUB_ID`; check timer logs.

## Next steps

- Add a Discord notifier after successful upsert to post activity summaries.
- Persist a cursor (last seen timestamp) to further reduce duplicates.
- Add tests and structured logging.
