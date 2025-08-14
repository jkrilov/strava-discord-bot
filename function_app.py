import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="strava_webhook")
def strava_webhook(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    token = req.params.get('VERIFY_TOKEN')
    if not token:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            token = req_body.get('VERIFY_TOKEN')

    if token == "STRAVA":
        return func.HttpResponse(
            "Validated token successfully.",
            status_code=200
        )
    else:
        return func.HttpResponse(
             "Missing token",
             status_code=400
        )
