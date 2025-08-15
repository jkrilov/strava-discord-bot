import json
import os
import logging
from datetime import datetime, timezone
import azure.functions as func
import requests
from azure.data.tables import TableServiceClient, UpdateMode
try:
    from azure.identity import DefaultAzureCredential  # type: ignore
except ImportError:  # azure-identity optional until installed
    DefaultAzureCredential = None  # type: ignore

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


TABLE_NAME = os.getenv("STRAVA_TOKENS_TABLE", "StravaTokens")


def _get_table_client():
    """Return a TableClient for the tokens table using managed identity.

    Only supports connection-stringless access via AzureWebJobsStorage__tableServiceUri
    and a system or user-assigned managed identity with 'Storage Table Data Contributor'.
    """
    table_uri = os.getenv("AzureWebJobsStorage__tableServiceUri")
    if table_uri:
        if DefaultAzureCredential is None:
            logging.error(
                "azure-identity not installed; cannot use identity-based storage access. "
                "Install azure-identity to use AzureWebJobsStorage__tableServiceUri."
            )
            return None
        try:
            # Support user-assigned managed identity by allowing explicit client ID env var
            # Azure Functions sets MSI_ENDPOINT / IDENTITY_ENDPOINT automatically for system identity.
            # For user-assigned identities, set AZURE_CLIENT_ID (preferred) or USER_ASSIGNED_MANAGED_IDENTITY_CLIENT_ID.
            user_client_id = (
                os.getenv("AZURE_CLIENT_ID")
                or os.getenv("USER_ASSIGNED_MANAGED_IDENTITY_CLIENT_ID")
            )
            if user_client_id:
                cred = DefaultAzureCredential(managed_identity_client_id=user_client_id)
            else:
                cred = DefaultAzureCredential()
            svc = TableServiceClient(endpoint=table_uri, credential=cred)
            try:
                svc.create_table_if_not_exists(TABLE_NAME)
            except Exception:  # noqa: BLE001
                pass
            return svc.get_table_client(TABLE_NAME)
        except Exception:  # noqa: BLE001
            logging.exception("Failed identity TableServiceClient initialization")
            return None

    logging.error("AzureWebJobsStorage__tableServiceUri not configured; cannot access Table Storage.")
    return None


def store_tokens(athlete_id: int, data: dict):
    """Persist tokens for an athlete in Azure Table Storage.

    Columns: PartitionKey, RowKey, access_token, refresh_token, expires_at, scope, updated_at.
    """
    table = _get_table_client()
    if not table:
        return False
    entity = {
        "PartitionKey": "athlete",
        "RowKey": str(athlete_id),
        "access_token": data.get("access_token"),
        "refresh_token": data.get("refresh_token"),
        "expires_at": int(data.get("expires_at") or 0),
        "scope": data.get("scope"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        table.upsert_entity(entity, mode=UpdateMode.MERGE)
        logging.info("Stored tokens for athlete %s", athlete_id)
        return True
    except Exception:
        logging.exception("Failed to store tokens for athlete %s", athlete_id)
        return False


def get_tokens(athlete_id: int):
    """Retrieve token entity for athlete. Returns dict or None."""
    table = _get_table_client()
    if not table:
        return None
    try:
        entity = table.get_entity(partition_key="athlete", row_key=str(athlete_id))
        return dict(entity)
    except Exception:
        return None


def refresh_tokens(athlete_id: int):
    """Refresh tokens if expired or within buffer. Returns (changed, entity)."""
    buffer_secs = int(os.getenv("STRAVA_REFRESH_BUFFER", "300"))  # 5 min default
    entity = get_tokens(athlete_id)
    if not entity:
        return False, None
    expires_at = int(entity.get("expires_at") or 0)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if expires_at - now_ts > buffer_secs:
        # Still valid; no refresh
        return False, entity
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    if not client_id or not client_secret:
        logging.error("Cannot refresh tokens; missing client credentials")
        return False, entity
    refresh_token = entity.get("refresh_token")
    if not refresh_token:
        logging.error("No refresh token stored for athlete %s", athlete_id)
        return False, entity
    token_url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    try:
        resp = requests.post(token_url, data=payload, timeout=10)
    except requests.RequestException:
        logging.exception("Network error refreshing tokens for athlete %s", athlete_id)
        return False, entity
    if resp.status_code != 200:
        logging.warning(
            "Failed to refresh tokens athlete=%s status=%s body=%s", athlete_id, resp.status_code, resp.text[:300]
        )
        return False, entity
    data = resp.json()
    store_tokens(athlete_id, data)
    new_entity = get_tokens(athlete_id) or entity
    logging.info("Refreshed tokens for athlete %s", athlete_id)
    return True, new_entity


@app.route(route="strava_webhook", methods=["GET"])
def strava_webhook(req: func.HttpRequest) -> func.HttpResponse:
    """GET verification endpoint for Strava subscription handshake."""
    verify_token_expected = os.getenv("STRAVA_VERIFY_TOKEN", "STRAVA")
    token = req.params.get('hub.verify_token')
    challenge = req.params.get('hub.challenge')
    mode = req.params.get('hub.mode')

    if token == verify_token_expected and mode == "subscribe" and challenge:
        logging.info("Strava webhook verification succeeded: mode=%s token_ok challenge=%s", mode, challenge)
        return func.HttpResponse(
            json.dumps({"hub.challenge": challenge}),
            status_code=200,
            mimetype="application/json"
        )
    logging.warning(
        "Strava webhook verification failed: mode=%s token=%s challenge=%s expected_token=%s",
        mode, token, challenge, verify_token_expected
    )
    return func.HttpResponse("Invalid request", status_code=400)


@app.route(route="strava_webhook", methods=["POST"])
def strava_webhook_events(req: func.HttpRequest) -> func.HttpResponse:
    """POST event receiver for Strava activity update/create/delete notifications."""
    logging.info("Strava event POST received")
    raw_body = req.get_body()
    if not raw_body:
        logging.warning("Strava event POST missing body")
        return func.HttpResponse("Missing body", status_code=400)

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        logging.exception("Invalid JSON in Strava event POST")
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Log key fields (avoid logging entire payload if large)
    aspect = payload.get("aspect_type")
    obj_type = payload.get("object_type")
    obj_id = payload.get("object_id")
    owner_id = payload.get("owner_id")
    logging.info(
        "Strava event received: aspect=%s object_type=%s object_id=%s owner_id=%s",
        aspect, obj_type, obj_id, owner_id
    )

    # Processing placeholder: fetch activity details via Strava API and forward to Discord webhook.

    return func.HttpResponse("OK", status_code=200)


@app.route(route="token_exchange", methods=["GET"])
def token_exchange(req: func.HttpRequest) -> func.HttpResponse:
    """OAuth callback endpoint to exchange Strava auth code for access + refresh tokens.

    Query params from Strava: code, scope, state, error.
    Redirect URI configured in Strava app must match /api/token_exchange
    """
    error = req.params.get("error")
    if error:
        logging.warning("Strava authorization error: %s", error)
        return func.HttpResponse(json.dumps({"error": error}), status_code=400, mimetype="application/json")

    code = req.params.get("code")
    if not code:
        return func.HttpResponse(json.dumps({"error": "missing_code"}), status_code=400, mimetype="application/json")

    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    if not client_id or not client_secret:
        logging.error("Missing STRAVA_CLIENT_ID or STRAVA_CLIENT_SECRET env vars")
        return func.HttpResponse(
            json.dumps({"error": "server_not_configured"}),
            status_code=500,
            mimetype="application/json"
        )

    token_url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
    }
    try:
        resp = requests.post(token_url, data=payload, timeout=10)
    except requests.RequestException as ex:
        logging.exception("Network error exchanging Strava code")
        return func.HttpResponse(
            json.dumps({"error": "network_error", "detail": str(ex)}),
            status_code=502,
            mimetype="application/json"
        )

    if resp.status_code != 200:
        logging.warning("Strava token exchange failed: status=%s body=%s", resp.status_code, resp.text[:500])
        return func.HttpResponse(
            json.dumps({"error": "strava_error", "status": resp.status_code, "body": resp.text[:200]}),
            status_code=resp.status_code,
            mimetype="application/json"
        )

    data = resp.json()
    athlete = data.get("athlete", {}) or {}
    athlete_id_raw = athlete.get("id")
    if athlete_id_raw is None:
        athlete_id = -1
    else:
        try:
            athlete_id = int(athlete_id_raw)  # type: ignore[arg-type]
        except Exception:
            athlete_id = -1
    store_tokens(athlete_id, data)

    atok = (data.get("access_token") or "")
    logging.info(
        "Strava token exchange success athlete=%s expires_at=%s access_token_suffix=%s",
        athlete_id,
        data.get("expires_at"),
        atok[-4:]
    )

    # Return friendly HTML (tokens are NOT exposed)
    html_template = """<!doctype html>
<html lang='en'>
<head>
    <meta charset='utf-8'/>
    <title>Strava Connected</title>
    <meta name='viewport' content='width=device-width,initial-scale=1'/>
    <style>
        :root {{ font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; color:#222; background:#f5f7fa; }}
        body {{ margin:0; padding:2rem; display:flex; align-items:center; justify-content:center; min-height:100vh; }}
        .card {{ background:#fff; padding:2.25rem 2.5rem; border-radius:20px; max-width:560px; box-shadow:0 4px 24px -4px rgba(0,0,0,0.15); }}
        h1 {{ margin-top:0; font-size:1.9rem; display:flex; align-items:center; gap:.6rem; }}
        .logo {{ width:42px; height:42px; display:inline-block; }}
        p {{ line-height:1.5; }}
        code {{ background:#eef2f6; padding:.2rem .45rem; border-radius:4px; font-size:.85rem; }}
        .meta {{ margin-top:1.25rem; font-size:.8rem; color:#555; }}
    </style>
</head>
<body>
    <div class='card'>
        <h1>
            <span class='logo'>
                <svg viewBox='0 0 120 120' width='42' height='42' xmlns='http://www.w3.org/2000/svg'>
                    <defs>
                        <linearGradient id='g' x1='0' x2='1' y1='0' y2='1'>
                            <stop stop-color='#ff7e36'/>
                            <stop offset='1' stop-color='#fc4c02'/>
                        </linearGradient>
                    </defs>
                    <rect rx='24' width='120' height='120' fill='url(#g)'/>
                    <path d='M60 22 32 78h16l12-25 12 25h16L60 22Z' fill='#fff' fill-rule='evenodd'/>
                </svg>
            </span>
            Strava Connected
        </h1>
        <p>Your Strava account is now linked. You can close this tab.</p>
        <p>We'll post your activities automatically. If you ever want to revoke access, disconnect it in Strava's settings.</p>
        <div class='meta'>Athlete ID: <code>{athlete_id}</code></div>
    </div>
</body>
</html>"""
    html = html_template.format(athlete_id=athlete_id)
    return func.HttpResponse(html, status_code=200, mimetype="text/html")


@app.route(route="refresh_token", methods=["POST"])
def refresh_token_endpoint(req: func.HttpRequest) -> func.HttpResponse:
    """Manually trigger token refresh for an athlete (athlete_id in JSON or query).

    NOTE: Consider securing this endpoint (e.g., function key / auth) before production.
    """
    athlete_id = None
    try:
        if req.method == "POST" and req.get_body():
            body = json.loads(req.get_body())
            athlete_id = body.get("athlete_id")
    except Exception:
        pass
    if athlete_id is None:
        athlete_id = req.params.get("athlete_id")
    try:
        athlete_id_int = int(athlete_id)
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "invalid_or_missing_athlete_id"}),
            status_code=400,
            mimetype="application/json"
        )
    changed, entity = refresh_tokens(athlete_id_int)
    if not entity:
        return func.HttpResponse(json.dumps({"error": "not_found"}), status_code=404, mimetype="application/json")
    response = {
        "athlete_id": athlete_id_int,
        "refreshed": changed,
        "expires_at": entity.get("expires_at"),
    }
    return func.HttpResponse(json.dumps(response), status_code=200, mimetype="application/json")


@app.route(route="deauthorize", methods=["POST"])
def deauthorize(req: func.HttpRequest) -> func.HttpResponse:
    """Revoke Strava access and delete stored tokens.

    Athlete id provided via JSON or query param. Calls Strava deauthorize endpoint.
    """
    athlete_id = None
    try:
        if req.get_body():
            body = json.loads(req.get_body())
            athlete_id = body.get("athlete_id")
    except Exception:
        pass
    if athlete_id is None:
        athlete_id = req.params.get("athlete_id")
    try:
        athlete_id_int = int(athlete_id)
    except Exception:
        return func.HttpResponse(json.dumps({"error": "invalid_or_missing_athlete_id"}), status_code=400)
    entity = get_tokens(athlete_id_int)
    if not entity:
        return func.HttpResponse(json.dumps({"error": "not_found"}), status_code=404)
    access_token = entity.get("access_token")
    status = "skipped"
    if access_token:
        try:
            resp = requests.post(
                "https://www.strava.com/oauth/deauthorize",
                params={"access_token": access_token},
                timeout=10
            )
            status = f"{resp.status_code}"
        except requests.RequestException:
            logging.exception("Deauthorize request failed for athlete %s", athlete_id_int)
            status = "network_error"
    # Delete entity
    table = _get_table_client()
    if table:
        try:
            table.delete_entity(partition_key="athlete", row_key=str(athlete_id_int))
            logging.info("Deleted tokens for athlete %s", athlete_id_int)
        except Exception:
            logging.exception("Failed deleting entity for athlete %s", athlete_id_int)
            return func.HttpResponse(json.dumps({"error": "delete_failed", "deauth_status": status}), status_code=500)
    return func.HttpResponse(json.dumps({"athlete_id": athlete_id_int, "deauth_status": status}), status_code=200)


