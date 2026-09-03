import asyncio
import json
import re

import pytest
from aiormq import AMQPConnectionError

from rag_indexer import transport
from rag_indexer.main import main
from rag_indexer.transport import _file_metadata, publish_to_dlq


async def _await_dlq_callbacks() -> None:
    """The failure callback is now a detached fire-and-forget task; let it finish
    before asserting on the session it posted to.
    """
    await asyncio.gather(*transport._callback_tasks)


def test_file_metadata_narrows_to_what_cozy_reads():
    """The DLQ failure callback is narrowed to doc_rev/datetime/doctype -- what
    cozy's handler reads plus readability fields -- and drops everything else,
    notably secrets. OpenRAG's own success callback is not this narrow: it echoes
    every metadata key except its server-computed ones (UPLOAD_METADATA_SERVER_KEYS),
    so it also carries md5sum and app_metadata.
    """
    meta = {
        "doc_rev": "3-abc",
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
        "doc_rev": "3-abc",
        "datetime": "2026-01-15T12:00:00Z",
        "doctype": "io.cozy.files",
    }


def test_file_metadata_never_substitutes_md5sum_for_doc_rev():
    """cozy-stack answers 400 on an empty doc_rev, but silently drops a status
    whose doc_rev is not a revision -- a rejection is the safer of the two.
    """
    result = _file_metadata({"md5sum": "abc123"})
    assert result["doc_rev"] == ""


def test_file_metadata_app_metadata_cannot_override_doc_rev():
    result = _file_metadata({"doc_rev": "3-abc", "app_metadata": {"doc_rev": "9-forged"}})
    assert result["doc_rev"] == "3-abc"


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
    await _await_dlq_callbacks()

    call = session.post_call
    assert call["headers"] == {"Authorization": "Bearer cozy-secret-token"}
    assert call["url"] == "https://cozy.example/ai/index/status"
    # Never in a query string, never in the JSON body.
    assert "cozy-secret-token" not in call["url"]
    assert "cozy-secret-token" not in str(call["json"])
    assert len(channel.default_exchange.published) == 1


@pytest.mark.asyncio
async def test_publish_to_dlq_timestamp_matches_openrag_spelling():
    """Both callback producers must write the same timestamp: cozy-stack documents
    milliseconds and a "Z" suffix, not "+00:00" and microseconds.
    """
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {
        "partition": "p1",
        "file_id": "f1",
        "doc_rev": "3-abc",
        "callback_url": "https://cozy.example/ai/index/status",
    }

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)
    await _await_dlq_callbacks()

    timestamp = session.post_call["json"]["timestamp"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", timestamp), timestamp


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
    await _await_dlq_callbacks()

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
    await _await_dlq_callbacks()

    assert session.post_call["headers"] == {"Authorization": "Bearer tok-from-body"}


@pytest.mark.asyncio
async def test_publish_to_dlq_callback_token_not_echoed_in_payload_metadata():
    """_file_metadata must not echo the token back to cozy-stack."""
    result = _file_metadata({"doc_rev": "3-abc", "callback_token": "cozy-secret-token"})
    assert "callback_token" not in result
    assert "cozy-secret-token" not in str(result)
