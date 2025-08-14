import json
import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="strava_webhook", methods=["GET"])
def strava_webhook(req: func.HttpRequest) -> func.HttpResponse:
    """GET verification endpoint for Strava subscription handshake."""
    token = req.params.get('hub.verify_token')
    challenge = req.params.get('hub.challenge')
    mode = req.params.get('hub.mode')

    if token == "STRAVA" and mode == "subscribe" and challenge:
        logging.info("Strava webhook verification succeeded: mode=%s token=%s challenge=%s", mode, token, challenge)
        return func.HttpResponse(
            json.dumps({"hub.challenge": challenge}),
            status_code=200,
            mimetype="application/json"
        )
    logging.warning("Strava webhook verification failed: mode=%s token=%s challenge=%s", mode, token, challenge)
    return func.HttpResponse("Invalid request", status_code=400)


@app.route(route="strava_webhook", methods=["POST"])
def strava_webhook_events(req: func.HttpRequest) -> func.HttpResponse:
    """POST event receiver for Strava activity update/create/delete notifications."""
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
