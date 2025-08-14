import json
import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="strava_webhook")
def strava_webhook(req: func.HttpRequest) -> func.HttpResponse:
    """ Webhook for Strava API subscription """
    logging.info('Python HTTP trigger function processed a request.')

    # log request body
    logging.info("Request body: %s", req.get_body().decode())

    token = req.params.get('hub.verify_token')
    challenge = req.params.get('hub.challenge')
    mode = req.params.get('hub.mode')

    if token == "STRAVA" and mode == "subscribe" and challenge:
        logging.info("Received valid request: mode=%s, token=%s, challenge=%s", mode, token, challenge)
        # return a 200 response with the challenge in application/json format
        return func.HttpResponse(
            json.dumps({"hub.challenge": challenge}),
            status_code=200,
            mimetype="application/json"
        )
    else:
        logging.warning("Received invalid request: mode=%s, token=%s, challenge=%s", mode, token, challenge)
        return func.HttpResponse(
            "Invalid request",
            status_code=400
        )