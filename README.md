# Strava → Discord Bot (Azure Functions, Python)

Azure Functions (Python 3.12) app that polls a Strava club feed every 5 minutes and posts enhanced activity summaries to Discord. Features comprehensive logging, rate limiting, and smart message formatting.

## Features

- ⏰ **Timer-triggered** polling every 5 minutes (cron: `0 */5 * * * *`)
- 🔄 **OAuth token refresh** with automatic caching and retry logic
- 📊 **Enhanced Discord messages** with emojis, pace calculations, and workout type labels
- 🛡️ **Rate limiting protection** with custom Discord retry logic and exponential backoff
- 📈 **Application Insights logging** with structured telemetry and performance metrics
- 🔐 **User-assigned managed identity** authentication for Azure Table Storage
- 🎯 **Smart message handling** with batch processing and intelligent separators
- 🏃 **Sport-specific formatting** with appropriate pace/speed calculations and emojis

## Discord Message Format

Activities are posted with rich formatting:

```
🔥 John Doe
🏅 Morning Run
🏃 Run - Race
📏 Distance: 5.24 mi
⚡ Pace: 7:32/mi
⏱️ Total time: 39m
```

### Supported Sports & Emojis
- 🏃 Run / TrailRun
- 🚴 Ride (shows both pace and speed)
- 🏊 Swim (pace per 100 yards)
- 🚶 Walk / 🥾 Hike
- 🏓 Pickleball
- 💪 Workout / 🏋️ WeightTraining
- 🧘 Yoga / 🚣 Rowing

### Workout Type Labels
When available, workout types are appended to sport type:
- **Running**: Race, Long Run, Workout
- **Cycling**: Race, Workout, Time Trial

## How it works

1. **Timer Function**: Triggers every 5 minutes via Azure Functions timer
2. **OAuth Refresh**: Automatically refreshes Strava access token using refresh token with caching
3. **Activity Fetching**: Calls `GET /api/v3/clubs/{club_id}/activities` (first page, 20 activities)
4. **Unique ID Generation**: Creates hash-based IDs from athlete name + activity metrics
5. **Change Detection**: Compares with stored data to detect new/updated activities
6. **Discord Posting**: Posts new activities or updates existing messages with enhanced formatting
7. **Azure Storage**: Persists activity data using user-assigned managed identity
8. **Comprehensive Logging**: Tracks performance, errors, and operations in Application Insights

### Rate Limiting & Reliability
- **Discord Rate Limiting**: Custom retry decorator with exponential backoff (respects Retry-After headers)
- **Strava API Retries**: Tenacity-based retry logic for API failures
- **Batch Processing**: Smart separator handling for multiple activities
- **Error Recovery**: Comprehensive exception handling with structured logging

## Environment Variables

### Required Configuration
- `STRAVA_CLIENT_ID` — OAuth application client ID
- `STRAVA_CLIENT_SECRET` — OAuth application client secret  
- `STRAVA_REFRESH_TOKEN` — OAuth refresh token for API access
- `STRAVA_CLUB_ID` — Club ID to poll for activities
- `DISCORD_WEBHOOK_URL` — Discord webhook URL for posting messages

### Azure Storage (User-Assigned Managed Identity)
- `AzureWebJobsStorage__tableServiceUri` — Table storage service URI
- `AzureWebJobsStorage__clientId` — User-assigned managed identity client ID

### Optional Configuration
- `STRAVA_ACTIVITIES_TABLE` — Table name (default: `StravaActivities`)

## Managed Identity and RBAC

The function app uses **user-assigned managed identity** for secure access to Azure Table Storage:

1. **Create User-Assigned Managed Identity**: Create in Azure portal or via CLI
2. **Assign to Function App**: Configure the function app to use the managed identity
3. **Grant Storage Permissions**: Assign `Storage Table Data Contributor` role on the storage account
4. **Configure Environment**: Set `AzureWebJobsStorage__clientId` to the managed identity client ID

## Data Model (Azure Table Storage)

**Table**: `StravaActivities`

**Partition Strategy**: Athlete name (enables efficient queries per athlete)
- **PartitionKey**: `{athlete_firstname} {athlete_lastname}` (fallback: `unknown_athlete`)
- **RowKey**: MD5 hash of `athlete_name:distance:moving_time:elapsed_time`

**Stored Fields**:
- Activity details: `activity_name`, `sport_type`, `workout_type`
- Metrics: `distance`, `moving_time`, `elapsed_time`, `total_elevation_gain`
- Athlete info: `athlete_firstname`, `athlete_lastname`
- Discord tracking: `discord_message_id` (for message updates)
- Timestamps: `last_updated` (UTC)

## Application Insights Logging

Comprehensive structured logging with custom properties:

### Performance Metrics
- Execution duration and activity processing counts
- Token refresh timing and cache hit rates
- Discord API response times and retry attempts

### Operational Intelligence  
- Activity processing results (posted/updated/skipped/failed)
- Rate limiting events with retry delays
- Authentication state and token expiration tracking

### Error Tracking
- API failures with status codes and retry attempts
- Authentication errors with detailed context
- Discord webhook failures with message correlation

## Local Development

**Prerequisites**:
- Python 3.12 with Azure Functions Core Tools
- User-assigned managed identity with table access
- Strava OAuth application with refresh token

**Configuration**:
```bash
# Install dependencies
pip install -r requirements.txt

# Configure local settings (local.settings.json)
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "STRAVA_CLIENT_ID": "your_client_id",
    "STRAVA_CLIENT_SECRET": "your_client_secret", 
    "STRAVA_REFRESH_TOKEN": "your_refresh_token",
    "STRAVA_CLUB_ID": "your_club_id",
    "DISCORD_WEBHOOK_URL": "your_discord_webhook",
    "AzureWebJobsStorage__tableServiceUri": "https://youraccount.table.core.windows.net/",
    "AzureWebJobsStorage__clientId": "your_managed_identity_client_id"
  }
}

# Start local development
func start
```

## Troubleshooting

### Common Issues

**Authentication Errors**:
- Ensure user-assigned managed identity has `Storage Table Data Contributor` role
- Verify `AzureWebJobsStorage__clientId` matches your managed identity client ID
- Allow time for RBAC propagation (up to 30 minutes)

**Strava API Issues**:
- Validate `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`, and `STRAVA_REFRESH_TOKEN`
- Check `STRAVA_CLUB_ID` is correct and accessible with your token
- Monitor Application Insights for token refresh failures

**Discord Posting Problems**:
- Verify `DISCORD_WEBHOOK_URL` is correct and active
- Check for rate limiting in logs (429 errors)
- Ensure webhook has permissions to post messages

**No Activities Processing**:
- Confirm club has recent activities (within last 20 activities)
- Check timer function is executing (should see logs every 5 minutes)
- Verify Strava club ID and access permissions

### Monitoring

Use Application Insights to monitor:
- Function execution frequency and duration
- Activity processing success/failure rates
- Discord API response times and errors
- Token refresh cycles and cache hit rates

## Deployment

1. **Create Azure Resources**:
   - Function App (Python 3.12)
   - Storage Account
   - Application Insights
   - User-Assigned Managed Identity

2. **Configure Function App**:
   - Assign user-assigned managed identity
   - Set environment variables
   - Enable Application Insights

3. **Set RBAC Permissions**:
   - Grant managed identity `Storage Table Data Contributor` on storage account

4. **Deploy Function**:
   ```bash
   func azure functionapp publish your-function-app-name
   ```

## Dependencies

See `requirements.txt` for full dependency list:
- `azure-functions` - Azure Functions runtime
- `azure-identity` - Managed identity authentication
- `azure-data-tables` - Table storage client
- `requests` - HTTP client for Strava and Discord APIs
- `tenacity` - Retry logic for API calls

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Azure Timer   │───►│ Function App │───►│  Strava API     │
│   (5 minutes)   │    │              │    │  (Club Feed)    │
└─────────────────┘    └──────┬───────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Table Storage   │    │  Discord API    │
                       │ (Activities)    │    │  (Webhook)      │
                       └─────────────────┘    └─────────────────┘
                              │                        ▲
                              └────────────────────────┘
                                  (Activity Updates)
```
