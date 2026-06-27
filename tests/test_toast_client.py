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
        json={
            "token": {
                "tokenType": "Bearer",
                "accessToken": "abc123",
                "expiresIn": 86400,
            }
        },
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
    body = client.get(
        "/orders/v2/ordersBulk", params={"startDate": "x", "endDate": "y"}
    )

    assert body == [{"guid": "order-1"}]
    get_request = responses.calls[1].request
    assert get_request.headers["Authorization"] == "Bearer abc123"
    assert get_request.headers["Toast-Restaurant-External-ID"] == "guid-123"


@responses.activate
def test_get_paginated_follows_pages_until_short_page():
    responses.add(
        responses.POST,
        LOGIN_URL,
        json={"token": {"accessToken": "abc123"}},
        status=200,
    )
    responses.add(
        responses.GET,
        "https://ws-api.toasttab.com/orders/v2/ordersBulk",
        json=[{"guid": "a"}, {"guid": "b"}],
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {"businessDate": "20260625", "pageSize": "2", "page": "1"}
            )
        ],
    )
    responses.add(
        responses.GET,
        "https://ws-api.toasttab.com/orders/v2/ordersBulk",
        json=[{"guid": "c"}],
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {"businessDate": "20260625", "pageSize": "2", "page": "2"}
            )
        ],
    )
    client = ToastClient(CONFIG)
    out = client.get_paginated(
        "/orders/v2/ordersBulk", {"businessDate": "20260625"}, page_size=2
    )
    assert [o["guid"] for o in out] == ["a", "b", "c"]
