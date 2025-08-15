# Strava → Discord Bot (Azure Functions, Python)

Small Azure Functions (Python 3.12) app that integrates with Strava:
- Handles Strava webhook verification (GET) and events (POST)
- Performs OAuth code → token exchange and persists tokens
- Stores activities and updates in Azure Table Storage
- Uses Managed Identity for storage access (user- or system-assigned)

## Architecture

- Azure Functions (Python isolated) with HTTP triggers
- Storage: Azure Table Storage
  - Tokens table: `StravaTokens` (configurable)
  - Activities table: `StravaActivities` (configurable)
- Auth to storage: Managed Identity
- Strava OAuth: Authorization Code flow

## Endpoints (and auth levels)

- GET `/api/strava_webhook` (Anonymous)
  - Strava verification handshake. Expects `hub.mode=subscribe`, `hub.verify_token`, `hub.challenge`.
- POST `/api/strava_webhook` (Anonymous)
  - Receives activity events. On create/update: refresh tokens if needed, fetch details, upsert to activities. On delete: mark `deleted=True`.
- GET `/api/token_exchange` (Anonymous)
  - Exchanges Strava `code` for tokens and stores them. Returns a friendly HTML confirmation.
- POST `/api/refresh_token` (Function key)
  - Manually refresh tokens for an athlete. `athlete_id` in JSON body or query.
- POST `/api/deauthorize` (Function key)
  - Calls Strava deauthorize and deletes the stored token entity. `athlete_id` in JSON body or query.

Use `x-functions-key` header (recommended) or `?code=` when calling secured endpoints.

## Configuration

Strava:
- `STRAVA_CLIENT_ID`
- `STRAVA_CLIENT_SECRET`
- `STRAVA_VERIFY_TOKEN`

Tables:
- `STRAVA_TOKENS_TABLE` (default: `StravaTokens`)
- `STRAVA_ACTIVITIES_TABLE` (default: `StravaActivities`)
- `STRAVA_REFRESH_BUFFER` (seconds; default: `300`)

Managed Identity (no connection string):
- `AzureWebJobsStorage__tableServiceUri` (e.g., `https://<storage>.table.core.windows.net`)
- `AzureWebJobsStorage__clientId` (UAMI client ID; omit for system-assigned)

RBAC:
- Assign `Storage Table Data Contributor` to the identity on the storage account.

## Table schemas

Tokens (`STRAVA_TOKENS_TABLE`):
- PK: `athlete`
- RK: `<athlete_id>`
- Cols: `access_token`, `refresh_token`, `expires_at`, `scope`, `updated_at`

Activities (`STRAVA_ACTIVITIES_TABLE`):
- PK: `<athlete_id>`
- RK: `<activity_id>`
- Event cols: `object_type`, `aspect_type`, `event_time`, `updated_at`, `updates_json`, `deleted`
- Details (when available): `name`, `sport_type`/`type`, `distance`, `moving_time`, `elapsed_time`, `start_date`, `start_date_local`, `private`, `visibility`

## Strava app setup

- Redirect URI → `/api/token_exchange`
- Webhook URL → `/api/strava_webhook`
- Verify token → `STRAVA_VERIFY_TOKEN`
- Scopes: `read`, `activity:read`, `activity:read_all`

## Local dev

This app uses managed identity only. Prefer testing in Azure. If you need local storage access, we can add an optional dev-only fallback (not included by design).

## Troubleshooting

- Credential warnings / invalid_scope:
  - Check `AzureWebJobsStorage__tableServiceUri` and (if UAMI) `AzureWebJobsStorage__clientId`
  - Ensure `Storage Table Data Contributor` on storage account
  - Restart Function App; wait a few minutes for RBAC propagation

- 403 AuthorizationPermissionMismatch:
  - Confirm role scope and identity
  - Pre-create tables in the portal if RBAC blocks programmatic creation

## Next steps

- Post activity summaries to Discord via webhook
- Optional `/connect` endpoint that redirects to Strava authorize URL
- Tests and error handling improvements

## Requirements

- Python 3.12+
- Libraries: `azure-functions`, `requests`, `azure-data-tables>=12.5.0`, `azure-identity>=1.17.0`
