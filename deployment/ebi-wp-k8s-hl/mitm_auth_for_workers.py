from os import getenv
import base64

from mitmproxy import http  # pip install mitmproxy


def request(flow: http.HTTPFlow) -> None:
    """
    Add a Basic auth header to API Calls to the prefect server's API.
    :param flow: intercepted MITMProxy http request
    :return: None
    """
    api = getenv("PREFECT_API_URL")
    if not api:
        raise UserWarning("PREFECT_API_URL env var not set")
    username = getenv("PREFECT_API_AUTH_USERNAME")
    password = getenv("PREFECT_API_AUTH_PASSWORD")
    if not username or not password:
        raise UserWarning("Prefect auth env vars not set")

    if flow.request.pretty_url.startswith(api):
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        flow.request.headers["Authorization"] = f"Basic {encoded_credentials}"
