"""E2E tests: full pipeline through RabbitMQ -> consumer -> RAG API."""

import asyncio
import hashlib
import json

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from aio_pika import Message, DeliveryMode

from rag_indexer.config import DLQ_NAME
from rag_indexer.errors import TransientError
from rag_indexer.processing import extract_metadata, get_retry_count
from rag_indexer.transport import publish_to_dlq

from tests.e2e.conftest import (
    publish_msg,
    consume_and_process,
    consume_and_process_cozy_json,
    publish_cozy_json_msg,
    TESTFILE_CONTENT,
    E2E_EXCHANGE,
)

pytestmark = [
    pytest.mark.usefixtures("require_docker"),
    pytest.mark.asyncio(loop_scope="module"),
]


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _make_upsert_headers(partition, file_id, md5sum, rag_base_url, name="test.txt"):
    return {
        "action": "upsert",
        "partition": partition,
        "file_id": file_id,
        "md5sum": md5sum,
        "name": name,
        "content_type": "text/plain",
        "rag_base_url": rag_base_url,
        "rag_api_key": "",
        # File content is always fetched via file_url (never sent as a body); the
        # stub's /testfile endpoint serves TESTFILE_CONTENT.
        "file_url": f"{rag_base_url}/testfile",
    }


async def test_upsert_new_file(rmq, rag_base_url, rag_state):
    """Publish upsert for a new file -> consumer POSTs to RAG -> file stored."""
    channel, exchange, queue = rmq
    body = b"Hello, this is a test document."
    md5 = _md5(body)
    headers = _make_upsert_headers("e2e-test-auto", "doc-1", md5, rag_base_url)

    await publish_msg(exchange, body, headers)
    await consume_and_process(queue, rag_base_url)

    # Verify file was stored in RAG
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rag_base_url}/partition/e2e-test-auto/file/doc-1") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["metadata"]["version"] == md5


async def test_upsert_forwards_callback_url(rmq, rag_base_url, rag_state):
    """Upsert with a callback_url header -> forwarded to RAG as a form field."""
    channel, exchange, queue = rmq
    body = b"Document with a status callback."
    md5 = _md5(body)
    callback_url = "https://cozy.example/status/cb-1"
    headers = _make_upsert_headers("e2e-test-auto", "doc-cb", md5, rag_base_url)
    headers["callback_url"] = callback_url

    await publish_msg(exchange, body, headers)
    await consume_and_process(queue, rag_base_url)

    # File stored AND the callback_url reached the RAG stub as a form field.
    posts = [c for c in rag_state.call_log if c["method"] == "POST" and c["file_id"] == "doc-cb"]
    assert posts, "Expected a POST for doc-cb"
    assert posts[-1].get("callback_url") == callback_url


async def test_idempotent_skip(rmq, rag_base_url, rag_state):
    """Same file+md5 sent again -> consumer skips (no PUT)."""
    channel, exchange, queue = rmq
    body = b"Hello, this is a test document."
    md5 = _md5(body)
    headers = _make_upsert_headers("e2e-test-auto", "doc-1", md5, rag_base_url)

    # Record call count before
    calls_before = len([c for c in rag_state.call_log if c["method"] in ("POST", "PUT")])

    await publish_msg(exchange, body, headers)
    await consume_and_process(queue, rag_base_url)

    # No POST or PUT should have been made (only GET for version check)
    calls_after = len([c for c in rag_state.call_log if c["method"] in ("POST", "PUT")])
    assert calls_after == calls_before, "Expected no POST/PUT on idempotent skip"


async def test_update_existing_file(rmq, rag_base_url, rag_state):
    """Same file_id with different content -> consumer PUTs update."""
    channel, exchange, queue = rmq
    new_body = b"Updated content for the test document."
    new_md5 = _md5(new_body)
    headers = _make_upsert_headers("e2e-test-auto", "doc-1", new_md5, rag_base_url)

    await publish_msg(exchange, new_body, headers)
    await consume_and_process(queue, rag_base_url)

    # Verify file was updated in RAG
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rag_base_url}/partition/e2e-test-auto/file/doc-1") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["metadata"]["version"] == new_md5


async def test_delete_file(rmq, rag_base_url):
    """Delete existing file -> consumer DELETEs in RAG -> file gone."""
    channel, exchange, queue = rmq
    headers = {
        "action": "delete",
        "partition": "e2e-test-auto",
        "file_id": "doc-1",
        "rag_base_url": rag_base_url,
        "rag_api_key": "",
    }

    await publish_msg(exchange, b"", headers)
    await consume_and_process(queue, rag_base_url)

    # Verify file is gone
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rag_base_url}/partition/e2e-test-auto/file/doc-1") as resp:
            assert resp.status == 404


async def test_delete_nonexistent(rmq, rag_base_url):
    """Delete non-existent file -> 404 treated as success (idempotent)."""
    channel, exchange, queue = rmq
    headers = {
        "action": "delete",
        "partition": "e2e-test-auto",
        "file_id": "no-such-file",
        "rag_base_url": rag_base_url,
        "rag_api_key": "",
    }

    await publish_msg(exchange, b"", headers)
    # Should not raise — 404 is treated as success
    await consume_and_process(queue, rag_base_url)


async def test_transient_error_on_unreachable_rag(rmq):
    """Unreachable RAG URL -> TransientError raised."""
    channel, exchange, queue = rmq
    body = b"some data"
    headers = {
        "action": "upsert",
        "partition": "e2e-test-auto",
        "file_id": "fail-doc",
        "md5sum": _md5(body),
        "content_type": "text/plain",
        "rag_base_url": "http://localhost:1",
        "rag_api_key": "",
    }

    await publish_msg(exchange, body, headers)
    with pytest.raises(TransientError, match="[Nn]etwork error|[Cc]onnect"):
        await consume_and_process(queue, "http://localhost:1")


# ---------------------------------------------------------------------------
# Cozy-json format tests
# ---------------------------------------------------------------------------

async def test_cozy_json_happy_path(rmq, rag_base_url, rag_state):
    """Cozy-json format: all business fields in JSON body, AMQP headers empty.

    Verifies that extract_metadata reads from the body JSON and that the consumer
    builds IndexMessage correctly, then forwards callback_url to OpenRAG.
    """
    channel, exchange, queue = rmq
    callback_url = "https://cozy.example/status/cozy-json-happy"
    md5 = _md5(TESTFILE_CONTENT)

    await publish_cozy_json_msg(
        exchange,
        rag_base_url,
        partition="e2e-cozy-json",
        file_id="cozy-happy-doc",
        md5sum=md5,
        callback_url=callback_url,
    )

    await consume_and_process_cozy_json(queue)

    # File stored in RAG with correct version
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rag_base_url}/partition/e2e-cozy-json/file/cozy-happy-doc") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["metadata"]["version"] == md5

    # callback_url was forwarded to OpenRAG as a multipart form field
    posts = [c for c in rag_state.call_log if c["method"] == "POST" and c["file_id"] == "cozy-happy-doc"]
    assert posts, "Expected a POST for cozy-happy-doc"
    assert posts[-1].get("callback_url") == callback_url


async def test_cozy_json_dlq_fail(rmq, rag_base_url):
    """Cozy-json format + DLQ: publish_to_dlq reads callback_url/partition/file_id from body.

    Verifies that:
    - The failed-status POST is sent to the callback_url found in the JSON body.
    - The DLQ message body is the original JSON (wire format preserved).
    """
    channel, exchange, queue = rmq

    # Start an in-process callback stub to capture the failed-status POST.
    callback_received = []

    async def _callback(request: web.Request) -> web.Response:
        callback_received.append(await request.json())
        return web.Response(status=200)

    stub_app = web.Application()
    stub_app.router.add_post("/cb-dlq", _callback)
    callback_server = TestServer(stub_app)
    await callback_server.start_server()

    try:
        callback_url = str(callback_server.make_url("/cb-dlq"))

        # Declare a test DLQ queue so publish_to_dlq has somewhere to land.
        dlq_queue = await channel.declare_queue(DLQ_NAME, durable=False, auto_delete=True)

        body_json = await publish_cozy_json_msg(
            exchange,
            rag_base_url,
            partition="e2e-cozy-dlq",
            file_id="cozy-dlq-doc",
            callback_url=callback_url,
        )

        msg = await asyncio.wait_for(queue.get(no_ack=False), timeout=5)
        async with aiohttp.ClientSession() as session:
            await publish_to_dlq(channel, msg, session)
        await msg.ack()

        # Give the async callback POST a moment to complete.
        await asyncio.sleep(0.2)

        # Failed-status callback was POSTed to callback_url from the JSON body.
        assert len(callback_received) == 1
        cb = callback_received[0]
        assert cb["indexed"] is False
        assert cb["partition"] == "e2e-cozy-dlq"
        assert cb["file_id"] == "cozy-dlq-doc"

        # DLQ message body is the original JSON (body preserved, format intact).
        dlq_msg = await asyncio.wait_for(dlq_queue.get(no_ack=True), timeout=5)
        assert dlq_msg.body == body_json

    finally:
        await callback_server.close()


async def test_cozy_json_retry_xdeath_fallback(rmq):
    """Cozy-json format after retry: x-death AMQP headers don't block body JSON parsing.

    RabbitMQ adds x-death (and related) headers when dead-lettering through TTL retry
    queues. A naive 'if not headers: parse body' heuristic would silently fall back to
    the (empty of business fields) AMQP headers and lose all metadata. This test
    publishes a cozy-json message pre-loaded with realistic x-death headers and verifies
    that extract_metadata still reads partition/file_id from the JSON body, while
    get_retry_count still reads x-death from the real AMQP headers.
    """
    channel, exchange, queue = rmq

    # RabbitMQ clears any client-supplied x-death header (it is broker-managed and
    # only populated during real dead-lettering). To get a genuine x-death we route
    # the message through an actual short-TTL queue that dead-letters into the E2E
    # queue -- exactly what the production retry topology does.
    retry_q_name = "e2e.test.retry.xdeath.q"
    await channel.declare_queue(
        retry_q_name,
        durable=False,
        auto_delete=False,
        arguments={
            "x-message-ttl": 500,
            "x-dead-letter-exchange": E2E_EXCHANGE,
            "x-dead-letter-routing-key": "e2e.test.file",
        },
    )

    # Publish a cozy-json message (business fields in body) into the retry queue.
    payload = {
        "action": "upsert",
        "partition": "cozy-retry-part",
        "file_id": "cozy-retry-file",
        "rag_base_url": "http://placeholder",  # not used -- test stops at metadata
        "rag_api_key": "",
        "file_url": "http://placeholder/testfile",
    }
    body_json = json.dumps(payload).encode()
    await channel.default_exchange.publish(
        Message(
            body=body_json,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=retry_q_name,
    )

    # Wait for the TTL to expire; the broker dead-letters into the E2E queue and
    # stamps a real x-death entry with reason='expired'.
    msg = None
    for _ in range(50):
        msg = await queue.get(no_ack=False, fail=False)
        if msg is not None:
            break
        await asyncio.sleep(0.1)
    assert msg is not None, "message was not dead-lettered into the e2e queue"

    try:
        # Despite non-empty AMQP headers (broker-added x-death etc.), extract_metadata
        # reads from body JSON because 'partition' is absent from the AMQP headers.
        metadata = extract_metadata(msg)
        assert metadata["partition"] == "cozy-retry-part"
        assert metadata["file_id"] == "cozy-retry-file"

        # get_retry_count reads the genuine x-death stamped by the broker.
        assert get_retry_count(msg) == 1
    finally:
        await msg.ack()
        await channel.queue_delete(retry_q_name)
