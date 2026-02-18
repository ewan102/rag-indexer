import asyncio

import pytest

from rag_indexer import rag_client
from rag_indexer.processing import process_message, get_retry_count, next_retry_queue
from rag_indexer.errors import TransientError, FatalError
from tests.conftest import FakeResp, DummyMessage


# ------------------------
# Tests next_retry_queue
# ------------------------
def test_next_retry_queue():
    """Verify correct queue selection for each retry stage."""
    assert next_retry_queue(0) == "rag.index.retry.30s.q"
    assert next_retry_queue(1) == "rag.index.retry.5m.q"
    assert next_retry_queue(2) == "rag.index.retry.1h.q"
    assert next_retry_queue(3) is None


# ------------------------
# DELETE path
# ------------------------
@pytest.mark.asyncio
async def test_delete_200_ok(monkeypatch, headers_base, aiohttp_session_stub):
    call_log = {}

    async def fake_delete(session, rag, partition, file_id):
        call_log["called"] = True
        return FakeResp(200, text_data="OK")

    monkeypatch.setattr(rag_client, "rag_delete", fake_delete)

    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    await process_message(msg, aiohttp_session_stub)
    assert call_log.get("called") is True


@pytest.mark.asyncio
async def test_delete_404_ok(monkeypatch, headers_base, aiohttp_session_stub):
    async def fake_delete(session, rag, partition, file_id):
        return FakeResp(404, text_data="Not Found")

    monkeypatch.setattr(rag_client, "rag_delete", fake_delete)

    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    # 404 doit passer sans exception
    await process_message(msg, aiohttp_session_stub)


@pytest.mark.asyncio
async def test_delete_5xx_retry_exception(
    monkeypatch, headers_base, aiohttp_session_stub
):
    async def fake_delete(session, rag, partition, file_id):
        return FakeResp(503, text_data="RAG down")

    monkeypatch.setattr(rag_client, "rag_delete", fake_delete)

    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(TransientError) as ei:
        await process_message(msg, aiohttp_session_stub)
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

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)

    headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
    msg = DummyMessage(body=b"data", headers=headers)

    with pytest.raises(TransientError) as ei:
        await process_message(msg, aiohttp_session_stub)
    assert "RAG GET 500" in str(ei.value)


@pytest.mark.asyncio
async def test_upsert_get_4xx_raises_fatal_error(
    monkeypatch, headers_base, aiohttp_session_stub
):
    async def fake_get(session, rag, partition, file_id):
        return FakeResp(401, text_data="unauthorized")

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)

    headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
    msg = DummyMessage(body=b"data", headers=headers)

    with pytest.raises(FatalError) as ei:
        await process_message(msg, aiohttp_session_stub)
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
        # Simule un succes
        return None

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    body = b"PDFDATA"
    headers = {
        **headers_base,
        "action": "upsert",
        "md5sum": "abc123",
        "name": "doc.pdf",
        "doctype": "pdf",
    }
    msg = DummyMessage(body=body, headers=headers)

    await process_message(msg, aiohttp_session_stub)

    assert calls["upsert"] is not None
    assert calls["upsert"]["file_bytes"] == body
    assert calls["upsert"]["is_new"] is True
    # verifie quelques champs du message reconstruit par le consumer
    m = calls["upsert"]["msg"]
    assert m.file_id == headers_base["file_id"]
    assert m.partition == headers_base["partition"]


# ------------------------
# UPSERT path - GET 200 => md5 different => PUT (is_new=False)
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

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    body = b"PDFDATA"
    headers = {**headers_base, "action": "upsert", "md5sum": "NEW"}
    msg = DummyMessage(body=body, headers=headers)

    await process_message(msg, aiohttp_session_stub)

    assert calls["upsert"] is not None
    assert calls["upsert"]["file_bytes"] == body
    assert calls["upsert"]["is_new"] is False


# ------------------------
# UPSERT path - GET 200 => md5 identique => rien a faire
# ------------------------
@pytest.mark.asyncio
async def test_upsert_same_md5_skips(monkeypatch, headers_base, aiohttp_session_stub):
    called = {"upsert": False}

    async def fake_get(session, rag, partition, file_id):
        return FakeResp(200, json_data={"metadata": {"md5sum": "SAME"}})

    async def fake_upsert(session, msg, file_bytes, is_new):
        called["upsert"] = True

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    headers = {**headers_base, "action": "upsert", "md5sum": "SAME"}
    msg = DummyMessage(body=b"ignored", headers=headers)

    await process_message(msg, aiohttp_session_stub)
    assert called["upsert"] is False


# ------------------------
# ERRH-03: HTTP 429 raises TransientError
# ------------------------
@pytest.mark.asyncio
async def test_delete_429_raises_transient_error(monkeypatch, headers_base, aiohttp_session_stub):
    """HTTP 429 on delete path must raise TransientError, not FatalError."""
    async def fake_delete(session, rag, partition, file_id):
        return FakeResp(429, text_data="Rate limited")

    monkeypatch.setattr(rag_client, "rag_delete", fake_delete)
    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(TransientError):
        await process_message(msg, aiohttp_session_stub)


@pytest.mark.asyncio
async def test_upsert_get_429_raises_transient_error(monkeypatch, headers_base, aiohttp_session_stub):
    """HTTP 429 on upsert GET path must raise TransientError."""
    async def fake_get(session, rag, partition, file_id):
        return FakeResp(429, text_data="Rate limited")

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
    msg = DummyMessage(body=b"data", headers=headers)
    with pytest.raises(TransientError):
        await process_message(msg, aiohttp_session_stub)


# ------------------------
# ERRH-02: asyncio.TimeoutError wraps as TransientError
# ------------------------
@pytest.mark.asyncio
async def test_timeout_raises_transient_error(monkeypatch, headers_base, aiohttp_session_stub):
    """asyncio.TimeoutError must be caught and wrapped as TransientError."""
    async def fake_delete(session, rag, partition, file_id):
        raise asyncio.TimeoutError("Request timed out")

    monkeypatch.setattr(rag_client, "rag_delete", fake_delete)
    headers = {**headers_base, "action": "delete"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(TransientError, match="[Tt]imeout"):
        await process_message(msg, aiohttp_session_stub)


# ------------------------
# ERRH-01: Unknown action raises FatalError
# ------------------------
@pytest.mark.asyncio
async def test_unknown_action_raises_fatal_error(monkeypatch, headers_base, aiohttp_session_stub):
    """Unknown action must raise FatalError, not generic Exception."""
    headers = {**headers_base, "action": "invalid_action"}
    msg = DummyMessage(body=b"", headers=headers)
    with pytest.raises(FatalError, match="Unknown action"):
        await process_message(msg, aiohttp_session_stub)


# ------------------------
# get_retry_count: x-death parsing
# ------------------------
def test_get_retry_count_no_xdeath():
    """First delivery -- no x-death header -- retry count is 0."""
    msg = DummyMessage(body=b"", headers={})
    assert get_retry_count(msg) == 0


def test_get_retry_count_with_xdeath_entries():
    """x-death with 2 expired entries means retry count is 2."""
    msg = DummyMessage(body=b"", headers={
        "x-death": [
            {"queue": "rag.index.retry.30s.q", "reason": "expired"},
            {"queue": "rag.index.retry.5m.q", "reason": "expired"},
        ]
    })
    assert get_retry_count(msg) == 2


def test_get_retry_count_ignores_non_expired():
    """Only reason=expired entries count toward retry count."""
    msg = DummyMessage(body=b"", headers={
        "x-death": [
            {"queue": "rag.index.retry.30s.q", "reason": "expired"},
            {"queue": "rag.index.q", "reason": "rejected"},
        ]
    })
    assert get_retry_count(msg) == 1
