"""Integration tests for the full retry cycle against a real RabbitMQ broker.

Requires a running RabbitMQ instance (via docker-compose.test.yml).
Tests are skipped when the broker is not reachable.
"""

import asyncio
import hashlib
from datetime import datetime

import aio_pika
import aiohttp
import pytest
from aio_pika import Message, DeliveryMode
from aiohttp import web
from aiohttp.test_utils import TestServer

from rag_indexer import rag_client
from rag_indexer.errors import FatalError, TransientError
from rag_indexer.processing import process_message, get_retry_count, next_retry_queue
from rag_indexer.transport import publish_to_retry, publish_to_dlq

from tests.conftest import FakeResp
from tests.integration.conftest import (
    TEST_EXCHANGE,
    TEST_RETRY_QUEUES,
)


pytestmark = pytest.mark.usefixtures("require_broker")


async def _consume_one(queue, timeout: float = 15.0) -> aio_pika.IncomingMessage:
    async with queue.iterator() as it:
        msg = await asyncio.wait_for(it.__anext__(), timeout=timeout)
        return msg


def _make_message(
    file_id: str, body: bytes, *, name: str, content_type: str = "text/markdown", md5sum: str | None = None
) -> Message:
    headers = {
        "action": "upsert",
        "partition": "test-partition",
        "file_id": file_id,
        "rag_base_url": "http://fake-rag:8000",
        "rag_api_key": "",
        "content_type": content_type,
        "name": name,
    }
    if md5sum:
        headers["md5sum"] = md5sum
    return Message(
        body,
        headers=headers,
        delivery_mode=DeliveryMode.PERSISTENT,
    )


async def test_happy_path(rmq_channel, monkeypatch):
    """Message processed successfully on first attempt to the RAG"""
    channel, main_q, dlq = rmq_channel

    async def fake_get(*_):
        return FakeResp(404)

    async def fake_upsert(*_):
        return None

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    exchange = await channel.get_exchange(TEST_EXCHANGE)
    await exchange.publish(
        _make_message("happy-path-001", b"# Happy Path\n\nDocument de test.\n", name="happy_path.md"),
        routing_key="test.retry.index",
    )

    async with aiohttp.ClientSession() as session:
        msg = await _consume_one(main_q)
        assert get_retry_count(msg) == 0
        await process_message(msg, session)
        await msg.ack()

    assert await dlq.get(fail=False) is None


async def test_fatal_error_goes_to_dlq_immediately(rmq_channel, monkeypatch):
    """RAG 400 → FatalError → DLQ immediately, no retry queues touched."""
    channel, main_q, dlq = rmq_channel

    async def fake_get(*_):
        return FakeResp(400, text_data="Bad request")

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)

    exchange = await channel.get_exchange(TEST_EXCHANGE)
    await exchange.publish(
        _make_message("fatal-error-001", b"# Fatal\n", name="fatal.md"),
        routing_key="test.retry.index",
    )

    async with aiohttp.ClientSession() as session:
        msg = await _consume_one(main_q)
        assert get_retry_count(msg) == 0
        with pytest.raises(FatalError):
            await process_message(msg, session)
        await publish_to_dlq(channel, msg, session)
        await msg.ack()

    dlq_msg = await _consume_one(dlq, timeout=5.0)
    assert dlq_msg.body == b"# Fatal\n"
    await dlq_msg.ack()


async def test_transient_error_exhausts_retries_to_dlq(rmq_channel, monkeypatch):
    """RAG 5xx → TransientError → retry x3 → DLQ."""
    channel, main_q, dlq = rmq_channel

    async def fake_get(session, rag, partition, file_id):
        return FakeResp(503, text_data="RAG down")

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)

    exchange = await channel.get_exchange(TEST_EXCHANGE)
    await exchange.publish(
        Message(
            b"test-body",
            headers={
                "action": "upsert",
                "partition": "test-partition",
                "file_id": "retry-exhausted-001",
                "rag_base_url": "http://fake-rag:8000",
                "rag_api_key": "test-key",
                "content_type": "text/plain",
            },
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key="test.retry.index",
    )

    async with aiohttp.ClientSession() as session:
        for attempt in range(len(TEST_RETRY_QUEUES)):
            msg = await _consume_one(main_q)
            assert get_retry_count(msg) == attempt
            with pytest.raises(TransientError):
                await process_message(msg, session)
            await publish_to_retry(channel, msg, next_retry_queue(attempt))
            await msg.ack()

            ttl_ms = TEST_RETRY_QUEUES[attempt][1]
            await asyncio.sleep(ttl_ms / 1000.0 + 0.5)

        msg = await _consume_one(main_q)
        assert get_retry_count(msg) == len(TEST_RETRY_QUEUES)
        assert next_retry_queue(get_retry_count(msg)) is None
        with pytest.raises(TransientError):
            await process_message(msg, session)
        await publish_to_dlq(channel, msg, session)
        await msg.ack()

    # Verify message arrived in DLQ
    dlq_msg = await _consume_one(dlq, timeout=5.0)
    assert dlq_msg.body == b"test-body"
    await dlq_msg.ack()


async def test_dlq_failed_callback_is_posted(rmq_channel):
    """publish_to_dlq with a callback_url header -> message to DLQ AND an 'error'-status callback POST."""
    channel, main_q, dlq = rmq_channel

    # Local server standing in for the cozy webhook that receives the callback.
    received: list[dict] = []

    async def handler(request: web.Request) -> web.Response:
        received.append(await request.json())
        return web.json_response({}, status=200)

    app = web.Application()
    app.router.add_post("/cb", handler)
    server = TestServer(app)
    await server.start_server()
    callback_url = str(server.make_url("/cb"))

    try:
        # Publish a message carrying the callback_url + doc identity headers.
        exchange = await channel.get_exchange(TEST_EXCHANGE)
        body = b'{"test": "dlq-callback"}'
        headers = {
            "file_id": "doc-dlq",
            "partition": "user-dlq",
            "callback_url": callback_url,
            "version": "v1",
            "doctype": "io.cozy.files",
            "datetime": "2026-01-15T12:00:00Z",
        }
        await exchange.publish(
            Message(body, headers=headers, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key="test.retry.index",
        )
        msg = await _consume_one(main_q)

        async with aiohttp.ClientSession() as session:
            await publish_to_dlq(channel, msg, session)
        await msg.ack()

        # 1) Message landed in the DLQ.
        dlq_msg = await _consume_one(dlq, timeout=5.0)
        assert dlq_msg.body == body
        await dlq_msg.ack()

        # 2) The failure callback was POSTed with the expected body.
        assert len(received) == 1
        cb = received[0]
        assert set(cb.keys()) == {"partition", "file_id", "status", "timestamp", "metadata"}
        assert cb["partition"] == "user-dlq"
        assert cb["file_id"] == "doc-dlq"
        assert cb["status"] == "error"
        assert cb["metadata"] == {
            "version": "v1",
            "datetime": "2026-01-15T12:00:00Z",
            "doctype": "io.cozy.files",
        }
        datetime.fromisoformat(cb["timestamp"])  # parsable ISO 8601
    finally:
        await server.close()


async def test_transient_error_then_success_on_retry(rmq_channel, monkeypatch):
    """GET 503 on first attempt → retry → GET 404 + POST → success."""
    channel, main_q, dlq = rmq_channel
    call_count = 0

    async def flaky_get(session, rag, partition, file_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return FakeResp(503, text_data="RAG temporarily down")
        return FakeResp(404)

    async def fake_upsert(session, msg, is_new):
        assert is_new is True
        return None

    monkeypatch.setattr(rag_client, "rag_get_file", flaky_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    exchange = await channel.get_exchange(TEST_EXCHANGE)
    await exchange.publish(
        _make_message("retry-then-success-001", b"# Retry then success\n\nDocument test.\n", name="test_retry.md"),
        routing_key="test.retry.index",
    )

    async with aiohttp.ClientSession() as session:
        msg = await _consume_one(main_q)
        assert get_retry_count(msg) == 0
        with pytest.raises(TransientError):
            await process_message(msg, session)
        await publish_to_retry(channel, msg, next_retry_queue(0))
        await msg.ack()

        ttl_ms = TEST_RETRY_QUEUES[0][1]
        await asyncio.sleep(ttl_ms / 1000.0 + 0.5)

        msg = await _consume_one(main_q)
        assert get_retry_count(msg) == 1
        await process_message(msg, session)
        await msg.ack()

    assert await dlq.get(fail=False) is None


async def test_concurrent_messages_same_file(rmq_channel, monkeypatch):
    """2 messages with same file_id processed sequentially — second is a no-op (same version)."""
    channel, main_q, dlq = rmq_channel

    body = b"# Concurrent test\n\nMeme contenu.\n"
    md5sum = hashlib.md5(body).hexdigest()

    # Tiny in-memory RAG store: first GET misses (404), fake_upsert "persists"
    # the md5sum, second GET hits with a matching version so need_index=False.
    store: dict[str, str] = {}

    async def fake_get(session, rag, partition, file_id):
        if file_id in store:
            return FakeResp(200, json_data={"metadata": {"md5sum": store[file_id]}})
        return FakeResp(404)

    async def fake_upsert(session, msg, is_new):
        assert is_new is True
        store[msg.file_id] = msg.md5sum
        return None

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    exchange = await channel.get_exchange(TEST_EXCHANGE)
    for _ in range(2):
        await exchange.publish(
            _make_message("concurrent-same-file-001", body, name="concurrent.md", md5sum=md5sum),
            routing_key="test.retry.index",
        )

    async with aiohttp.ClientSession() as session:
        for _ in range(2):
            msg = await _consume_one(main_q, timeout=5.0)
            await process_message(msg, session)
            await msg.ack()

    assert await dlq.get(fail=False) is None
