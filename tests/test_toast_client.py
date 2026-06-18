import pytest
import responses

from ingest.config import ToastConfig
from ingest.toast_client import ToastAuthError, ToastClient

CONFIG = ToastConfig(
    base_url="https://ws-api.toasttab.com",
    client_id="cid",
    client_secret="sec",
    restaurant_guid="guid-123",
)
LOGIN_URL = "https://ws-api.toasttab.com/authentication/v1/authentication/login"


@responses.activate
def test_authenticate_returns_access_token():
    responses.add(
        responses.POST,
        LOGIN_URL,
        json={"token": {"tokenType": "Bearer", "accessToken": "abc123", "expiresIn": 86400}},
        status=200,
    )
    client = ToastClient(CONFIG)
    assert client.authenticate() == "abc123"


@responses.activate
def test_authenticate_raises_on_http_error():
    responses.add(responses.POST, LOGIN_URL, json={"error": "bad creds"}, status=401)
    client = ToastClient(CONFIG)
    with pytest.raises(ToastAuthError):
        client.authenticate()


@responses.activate
def test_get_sends_bearer_and_restaurant_header():
    responses.add(
        responses.POST,
        LOGIN_URL,
        json={"token": {"accessToken": "abc123"}},
        status=200,
    )
    responses.add(
        responses.GET,
        "https://ws-api.toasttab.com/orders/v2/ordersBulk",
        json=[{"guid": "order-1"}],
        status=200,
    )
    client = ToastClient(CONFIG)
    body = client.get("/orders/v2/ordersBulk", params={"startDate": "x", "endDate": "y"})

    assert body == [{"guid": "order-1"}]
    get_request = responses.calls[1].request
    assert get_request.headers["Authorization"] == "Bearer abc123"
    assert get_request.headers["Toast-Restaurant-External-ID"] == "guid-123"
