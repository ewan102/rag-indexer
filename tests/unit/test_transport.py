import json

import pytest
from aiormq import AMQPConnectionError

from rag_indexer.main import main
from rag_indexer.transport import _file_metadata, publish_to_dlq


def test_file_metadata_maps_fields_and_excludes_secrets():
    meta = {
        "md5sum": "abc123",
        "datetime": "2026-01-15T12:00:00Z",
        "doctype": "io.cozy.files",
        "app_metadata": {"custom": "value"},
        # sensitive fields that must never be echoed back:
        "callback_url": "https://cozy.example/cb",
        "rag": {"api_key": "secret"},
    }

    result = _file_metadata(meta)

    assert result == {
        "version": "abc123",  # md5sum surfaced as version
        "datetime": "2026-01-15T12:00:00Z",
        "doctype": "io.cozy.files",
        "custom": "value",  # app_metadata merged
    }


def test_file_metadata_prefers_explicit_version_over_md5sum():
    result = _file_metadata({"version": "v2", "md5sum": "abc123"})
    assert result["version"] == "v2"


# ------------------------
# BUGF-03: Exit code on connection failure
# ------------------------
@pytest.mark.asyncio
async def test_main_exits_with_nonzero_on_connection_failure(monkeypatch):
    async def fake_connect_robust(url):
        raise AMQPConnectionError("Connection refused")

    monkeypatch.setattr("aio_pika.connect_robust", fake_connect_robust)

    with pytest.raises(SystemExit) as exc_info:
        await main()
    assert exc_info.value.code == 1


# ------------------------
# callback_token: signs the DLQ failure callback
# ------------------------
class _FakeExchange:
    def __init__(self):
        self.published = []

    async def publish(self, message, routing_key=None):
        self.published.append((message, routing_key))


class _FakeChannel:
    def __init__(self):
        self.default_exchange = _FakeExchange()


class _FakeCallbackResp:
    def __init__(self, status: int = 200):
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeCallbackSession:
    """Fake ClientSession recording the failure-callback POST."""

    def __init__(self, status: int = 200):
        self._status = status
        self.post_call = None

    def post(self, url, json=None, headers=None, timeout=None):
        self.post_call = {"url": url, "json": json, "headers": headers}
        return _FakeCallbackResp(self._status)


class _DlqMessage:
    def __init__(self, body: bytes = b"{}", headers: dict | None = None):
        self.body = body
        self.headers = headers or {}
        self.content_type = "application/json"


@pytest.mark.asyncio
async def test_publish_to_dlq_signs_callback_with_bearer_token():
    """Sent as an Authorization bearer header, not in the URL."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {
        "partition": "p1",
        "file_id": "f1",
        "callback_url": "https://cozy.example/ai/index/status",
        "callback_token": "cozy-secret-token",
    }

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)

    call = session.post_call
    assert call["headers"] == {"Authorization": "Bearer cozy-secret-token"}
    assert call["url"] == "https://cozy.example/ai/index/status"
    # Never in a query string, never in the JSON body.
    assert "cozy-secret-token" not in call["url"]
    assert "cozy-secret-token" not in str(call["json"])
    assert len(channel.default_exchange.published) == 1


@pytest.mark.asyncio
async def test_publish_to_dlq_without_token_sends_no_auth_header():
    """Optional field: an untokenised message works as before."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {
        "partition": "p1",
        "file_id": "f1",
        "callback_url": "https://cozy.example/ai/index/status",
    }

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)

    assert session.post_call["headers"] is None
    assert session.post_call["json"]["status"] == "error"


@pytest.mark.asyncio
async def test_publish_to_dlq_reads_callback_token_from_cozy_json_body():
    """cozy-json wire format: token read from the body, not the AMQP headers."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    body = json.dumps({
        "partition": "p1",
        "file_id": "f1",
        "callback_url": "https://cozy.example/ai/index/status",
        "callback_token": "tok-from-body",
    }).encode()

    # metadata=None -> publish_to_dlq extracts it itself
    await publish_to_dlq(channel, _DlqMessage(body=body), session, metadata=None)

    assert session.post_call["headers"] == {"Authorization": "Bearer tok-from-body"}


@pytest.mark.asyncio
async def test_publish_to_dlq_callback_token_not_echoed_in_payload_metadata():
    """_file_metadata must not echo the token back to cozy-stack."""
    result = _file_metadata({"md5sum": "abc", "callback_token": "cozy-secret-token"})
    assert "callback_token" not in result
    assert "cozy-secret-token" not in str(result)
