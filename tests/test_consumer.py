import asyncio
import json
import types
import pytest
from aiormq import AMQPConnectionError

# On suppose que ton fichier s'appelle consumer.py
import consumer
from consumer import TransientError, FatalError


# ------------------------
# Helpers & fakes
# ------------------------
class FakeResp:
    def __init__(self, status: int, json_data=None, text_data=""):
        self.status = status
        self._json = json_data
        self._text = text_data

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    async def read(self):
        # le consumer attend parfois un "read()" pour vider le flux
        return b""


class FakeContextResp:
    """Async context manager that mimics aiohttp response for get_producer_file tests."""
    def __init__(self, status: int, body: bytes):
        self.status = status
        self._body = body

    async def read(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class FakeSession:
    """Fake aiohttp.ClientSession whose .get() returns a FakeContextResp.

    IMPORTANT: .get() must be a regular (synchronous) method, NOT async.
    aiohttp's session.get() returns a context manager object synchronously;
    the async part is the `async with` that calls __aenter__ on it.
    """
    def __init__(self, resp: FakeContextResp):
        self._resp = resp

    def get(self, url, headers=None, timeout=None):
        # Returns the FakeContextResp directly -- it implements __aenter__/__aexit__
        return self._resp


class DummyMessage:
    """Suffisant pour process_message(): expose .body et .headers."""

    def __init__(self, body: bytes, headers: dict):
        self.body = body
        self.headers = headers


@pytest.fixture
def headers_base():
    return {
        "partition": "user-1",
        "file_id": "file-123",
        "rag_base_url": "http://rag:8000",
        "rag_api_key": "secret",
        "content_type": "application/octet-stream",
    }


@pytest.fixture
def aiohttp_session_stub():
    """Le process_message reçoit un session aiohttp, mais comme on monkeypatch
    rag_get_file / rag_upsert / rag_delete, cette session ne sera pas réellement utilisée.
    """

    class _S:
        pass

    return _S()


# ------------------------
# Tests next_retry_queue
# ------------------------
def test_next_retry_queue():
    """Verify correct queue selection for each retry stage."""
    assert consumer.next_retry_queue(0) == "rag.index.retry.30s.q"
    assert consumer.next_retry_queue(1) == "rag.index.retry.5m.q"
    assert consumer.next_retry_queue(2) == "rag.index.retry.1h.q"
    assert consumer.next_retry_queue(3) is None


# ------------------------
# DELETE path
# ------------------------
@pytest.mark.asyncio
async def test_delete_200_ok(monkeypatch, headers_base, aiohttp_session_stub):
    call_log = {}

    async def fake_delete(session, rag, partition, file_id):
        call_log["called"] = True
        return FakeResp(200, text_data="OK")

    monkeypatch.setattr(consumer, "rag_delete", fake_delete)

    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    await consumer.process_message(msg, aiohttp_session_stub)
    assert call_log.get("called") is True


@pytest.mark.asyncio
async def test_delete_404_ok(monkeypatch, headers_base, aiohttp_session_stub):
    async def fake_delete(session, rag, partition, file_id):
        return FakeResp(404, text_data="Not Found")

    monkeypatch.setattr(consumer, "rag_delete", fake_delete)

    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    # 404 doit passer sans exception
    await consumer.process_message(msg, aiohttp_session_stub)


@pytest.mark.asyncio
async def test_delete_5xx_retry_exception(
    monkeypatch, headers_base, aiohttp_session_stub
):
    async def fake_delete(session, rag, partition, file_id):
        return FakeResp(503, text_data="RAG down")

    monkeypatch.setattr(consumer, "rag_delete", fake_delete)

    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(TransientError) as ei:
        await consumer.process_message(msg, aiohttp_session_stub)
    assert "RAG delete 503" in str(ei.value)


# ------------------------
# UPSERT path - GET 5xx / 4xx
# ------------------------
@pytest.mark.asyncio
async def test_upsert_get_5xx_raises_transient_error(
    monkeypatch, headers_base, aiohttp_session_stub
):
    async def fake_get(session, rag, partition, file_id):
        return FakeResp(500, text_data="boom")

    monkeypatch.setattr(consumer, "rag_get_file", fake_get)

    headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
    msg = DummyMessage(body=b"data", headers=headers)

    with pytest.raises(TransientError) as ei:
        await consumer.process_message(msg, aiohttp_session_stub)
    assert "RAG GET 500" in str(ei.value)


@pytest.mark.asyncio
async def test_upsert_get_4xx_raises_fatal_error(
    monkeypatch, headers_base, aiohttp_session_stub
):
    async def fake_get(session, rag, partition, file_id):
        return FakeResp(401, text_data="unauthorized")

    monkeypatch.setattr(consumer, "rag_get_file", fake_get)

    headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
    msg = DummyMessage(body=b"data", headers=headers)

    with pytest.raises(FatalError) as ei:
        await consumer.process_message(msg, aiohttp_session_stub)
    assert "RAG GET 401" in str(ei.value)


# ------------------------
# UPSERT path - GET 404 => POST (is_new=True)
# ------------------------
@pytest.mark.asyncio
async def test_upsert_new_on_404_triggers_post(
    monkeypatch, headers_base, aiohttp_session_stub
):
    calls = {"upsert": None}

    async def fake_get(session, rag, partition, file_id):
        return FakeResp(404)

    async def fake_upsert(session, msg, file_bytes, is_new):
        calls["upsert"] = {
            "file_bytes": file_bytes,
            "is_new": is_new,
            "msg": msg,
        }
        # Simule un succès
        return None

    monkeypatch.setattr(consumer, "rag_get_file", fake_get)
    monkeypatch.setattr(consumer, "rag_upsert", fake_upsert)

    body = b"PDFDATA"
    headers = {
        **headers_base,
        "action": "upsert",
        "md5sum": "abc123",
        "name": "doc.pdf",
        "doctype": "pdf",
    }
    msg = DummyMessage(body=body, headers=headers)

    await consumer.process_message(msg, aiohttp_session_stub)

    assert calls["upsert"] is not None
    assert calls["upsert"]["file_bytes"] == body
    assert calls["upsert"]["is_new"] is True
    # vérifie quelques champs du message reconstruit par le consumer
    m = calls["upsert"]["msg"]
    assert m.file_id == headers_base["file_id"]
    assert m.partition == headers_base["partition"]


# ------------------------
# UPSERT path - GET 200 => md5 différent => PUT (is_new=False)
# ------------------------
@pytest.mark.asyncio
async def test_upsert_update_on_md5_change_triggers_put(
    monkeypatch, headers_base, aiohttp_session_stub
):
    calls = {"upsert": None}

    async def fake_get(session, rag, partition, file_id):
        # Le document existe avec md5sum "OLD"
        return FakeResp(200, json_data={"metadata": {"md5sum": "OLD"}})

    async def fake_upsert(session, msg, file_bytes, is_new):
        calls["upsert"] = {"file_bytes": file_bytes, "is_new": is_new}
        return None

    monkeypatch.setattr(consumer, "rag_get_file", fake_get)
    monkeypatch.setattr(consumer, "rag_upsert", fake_upsert)

    body = b"PDFDATA"
    headers = {**headers_base, "action": "upsert", "md5sum": "NEW"}
    msg = DummyMessage(body=body, headers=headers)

    await consumer.process_message(msg, aiohttp_session_stub)

    assert calls["upsert"] is not None
    assert calls["upsert"]["file_bytes"] == body
    assert calls["upsert"]["is_new"] is False


# ------------------------
# UPSERT path - GET 200 => md5 identique => rien à faire
# ------------------------
@pytest.mark.asyncio
async def test_upsert_same_md5_skips(monkeypatch, headers_base, aiohttp_session_stub):
    called = {"upsert": False}

    async def fake_get(session, rag, partition, file_id):
        return FakeResp(200, json_data={"metadata": {"md5sum": "SAME"}})

    async def fake_upsert(session, msg, file_bytes, is_new):
        called["upsert"] = True

    monkeypatch.setattr(consumer, "rag_get_file", fake_get)
    monkeypatch.setattr(consumer, "rag_upsert", fake_upsert)

    headers = {**headers_base, "action": "upsert", "md5sum": "SAME"}
    msg = DummyMessage(body=b"ignored", headers=headers)

    await consumer.process_message(msg, aiohttp_session_stub)
    assert called["upsert"] is False


# ------------------------
# BUGF-01: File download reads inside context manager
# ------------------------
@pytest.mark.asyncio
async def test_get_producer_file_reads_inside_context():
    expected_bytes = b"file-content-here"
    fake_resp = FakeContextResp(status=200, body=expected_bytes)
    fake_session = FakeSession(fake_resp)

    msg = consumer.IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=consumer.RagConn(base_url="http://rag:8000", api_key="key"),
        content=consumer.ContentSpec(file_url="http://example.com/file.bin"),
    )
    result = await consumer.get_producer_file(fake_session, msg)
    assert result == expected_bytes


# ------------------------
# BUGF-02: Metadata merge
# ------------------------
def test_build_metadata_merges_app_metadata():
    msg = consumer.IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=consumer.RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        doctype="pdf",
        app_metadata={"custom_key": "custom_value", "author": "test"},
    )
    meta = consumer.build_metadata(msg)
    assert meta["custom_key"] == "custom_value"
    assert meta["author"] == "test"
    assert meta["version"] == "v1"
    assert meta["doctype"] == "pdf"


def test_build_metadata_without_app_metadata():
    msg = consumer.IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=consumer.RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        doctype="pdf",
    )
    meta = consumer.build_metadata(msg)
    assert meta["version"] == "v1"
    assert meta["doctype"] == "pdf"
    # No extra keys beyond the base fields
    assert "custom_key" not in meta


# ------------------------
# BUGF-03: Exit code on connection failure
# ------------------------
@pytest.mark.asyncio
async def test_main_exits_with_nonzero_on_connection_failure(monkeypatch):
    async def fake_connect_robust(url):
        raise AMQPConnectionError("Connection refused")

    monkeypatch.setattr("aio_pika.connect_robust", fake_connect_robust)

    with pytest.raises(SystemExit) as exc_info:
        await consumer.main()
    assert exc_info.value.code == 1


# ------------------------
# ERRH-03: HTTP 429 raises TransientError
# ------------------------
@pytest.mark.asyncio
async def test_delete_429_raises_transient_error(monkeypatch, headers_base, aiohttp_session_stub):
    """HTTP 429 on delete path must raise TransientError, not FatalError."""
    async def fake_delete(session, rag, partition, file_id):
        return FakeResp(429, text_data="Rate limited")

    monkeypatch.setattr(consumer, "rag_delete", fake_delete)
    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(TransientError):
        await consumer.process_message(msg, aiohttp_session_stub)


@pytest.mark.asyncio
async def test_upsert_get_429_raises_transient_error(monkeypatch, headers_base, aiohttp_session_stub):
    """HTTP 429 on upsert GET path must raise TransientError."""
    async def fake_get(session, rag, partition, file_id):
        return FakeResp(429, text_data="Rate limited")

    monkeypatch.setattr(consumer, "rag_get_file", fake_get)
    headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
    msg = DummyMessage(body=b"data", headers=headers)
    with pytest.raises(TransientError):
        await consumer.process_message(msg, aiohttp_session_stub)


# ------------------------
# ERRH-02: asyncio.TimeoutError wraps as TransientError
# ------------------------
@pytest.mark.asyncio
async def test_timeout_raises_transient_error(monkeypatch, headers_base, aiohttp_session_stub):
    """asyncio.TimeoutError must be caught and wrapped as TransientError."""
    async def fake_delete(session, rag, partition, file_id):
        raise asyncio.TimeoutError("Request timed out")

    monkeypatch.setattr(consumer, "rag_delete", fake_delete)
    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(TransientError, match="[Tt]imeout"):
        await consumer.process_message(msg, aiohttp_session_stub)


# ------------------------
# ERRH-01: Unknown action raises FatalError
# ------------------------
@pytest.mark.asyncio
async def test_unknown_action_raises_fatal_error(monkeypatch, headers_base, aiohttp_session_stub):
    """Unknown action must raise FatalError, not generic Exception."""
    headers = {**headers_base, "action": "invalid_action"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(FatalError, match="Unknown action"):
        await consumer.process_message(msg, aiohttp_session_stub)


# ------------------------
# get_retry_count: x-death parsing
# ------------------------
def test_get_retry_count_no_xdeath():
    """First delivery -- no x-death header -- retry count is 0."""
    msg = DummyMessage(body=b"", headers={})
    assert consumer.get_retry_count(msg) == 0


def test_get_retry_count_with_xdeath_entries():
    """x-death with 2 expired entries means retry count is 2."""
    msg = DummyMessage(body=b"", headers={
        "x-death": [
            {"queue": "rag.index.retry.30s.q", "reason": "expired"},
            {"queue": "rag.index.retry.5m.q", "reason": "expired"},
        ]
    })
    assert consumer.get_retry_count(msg) == 2


def test_get_retry_count_ignores_non_expired():
    """Only reason=expired entries count toward retry count."""
    msg = DummyMessage(body=b"", headers={
        "x-death": [
            {"queue": "rag.index.retry.30s.q", "reason": "expired"},
            {"queue": "rag.index.q", "reason": "rejected"},
        ]
    })
    assert consumer.get_retry_count(msg) == 1
