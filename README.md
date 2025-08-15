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

Optional:
- `STRAVA_SINCE_SECONDS` — Recent window in seconds (default: `3600`)
- `STRAVA_ACTIVITIES_TABLE` — Table name (default: `StravaActivities`)
- `DISCORD_WEBHOOK_URL` — Discord webhook to post activity summaries (if set)
- `DISCORD_EDIT_UPDATES` — `true`/`false`; when `true`, edits prior messages if the summary changes (default: `true`). Set `false` to always post new messages.
- `STRAVA_PER_PAGE` — Page size for club feed (default: `50`)
- `STRAVA_MAX_PAGES` — Max pages fetched each run (default: `10`)
- `STRAVA_USE_TIME_FILTER` — `true`/`false`; when `true`, attempts time filtering using `updated_at`/`start_date` (default: `true`)
- `STRAVA_MAX_ACTIVITIES` — Max activities processed per run (default: `0` for unlimited)

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

- Discord posting is enabled when `DISCORD_WEBHOOK_URL` is set:
  - Converts meters→miles and seconds→minutes.
  - Skips distance display when < 0.05 miles (e.g., weight training).
  - Posts new messages and edits prior messages if the activity name or summary changes (can be disabled with `DISCORD_EDIT_UPDATES=false`).
- Persist a cursor (last seen timestamp) to further reduce duplicates.
- Add tests and structured logging.

## Backfill older activities

- The club feed doesn’t guarantee a dedicated timestamp for all entries, but many items include `updated_at` or `start_date`.
- You can increase `STRAVA_MAX_PAGES` (and optionally disable `STRAVA_USE_TIME_FILTER=false`) to sweep older pages.
- Deduping is enforced by the synthetic RowKey, so repeated sweeps won’t create duplicates.
