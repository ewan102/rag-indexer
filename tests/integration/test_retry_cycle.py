"""Integration tests for the full retry cycle against a real RabbitMQ broker.

Requires a running RabbitMQ instance (via docker-compose.test.yml).
Tests are skipped when the broker is not reachable.
"""

import asyncio
from datetime import datetime

import aio_pika
import aiohttp
import pytest
from aio_pika import Message, DeliveryMode
from aiohttp import web
from aiohttp.test_utils import TestServer

from rag_indexer.processing import get_retry_count, next_retry_queue
from rag_indexer.transport import publish_to_retry, publish_to_dlq

from tests.integration.conftest import (
    TEST_EXCHANGE,
    TEST_RETRY_QUEUES,
)


pytestmark = pytest.mark.usefixtures("require_broker")


async def _consume_one(queue, timeout: float = 15.0) -> aio_pika.IncomingMessage:
    """Consume a single message from the queue with a timeout."""
    async with queue.iterator() as it:
        msg = await asyncio.wait_for(it.__anext__(), timeout=timeout)
        return msg


async def test_message_exhausts_retries_and_reaches_dlq(rmq_channel):
    """Full retry cycle: message -> retry queues (x3) -> DLQ.

    Uses shortened TTLs (1s, 2s, 3s) so the full cycle completes in ~6-10s.
    Simulates transient failures by manually routing to retry queues.
    """
    channel, main_q, dlq = rmq_channel

    # Publish a test message to the main queue via the topic exchange
    exchange = await channel.get_exchange(TEST_EXCHANGE)
    test_body = b'{"test": "retry-cycle"}'
    await exchange.publish(
        Message(
            test_body,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key="test.retry.index",
    )

    # Cycle through all retry queues
    for cycle in range(len(TEST_RETRY_QUEUES)):
        # Consume from main queue
        msg = await _consume_one(main_q)

        # Verify message body
        assert msg.body == test_body

        # Check retry count from x-death headers
        retry_count = get_retry_count(msg)
        assert retry_count == cycle, (
            f"Expected retry_count={cycle}, got {retry_count}"
        )

        # Get next retry queue
        retry_queue_name = next_retry_queue(retry_count)
        assert retry_queue_name is not None, (
            f"Expected a retry queue for count {retry_count}, got None"
        )

        # Simulate transient failure: publish to retry queue, ack original
        await publish_to_retry(channel, msg, retry_queue_name)
        await msg.ack()

        # Wait for TTL expiry + re-delivery to main queue
        # TTLs are 1s, 2s, 3s -- add buffer for processing
        ttl_ms = TEST_RETRY_QUEUES[cycle][1]
        await asyncio.sleep(ttl_ms / 1000.0 + 1.0)

    # Final consume: retry count == len(RETRY_QUEUES), retries exhausted
    msg = await _consume_one(main_q)
    assert msg.body == test_body

    retry_count = get_retry_count(msg)
    assert retry_count == len(TEST_RETRY_QUEUES)

    # next_retry_queue returns None when exhausted
    assert next_retry_queue(retry_count) is None

    # Route to DLQ. No callback_url header -> callback is skipped silently
    # (current prod behavior: cozy does not send the header yet).
    async with aiohttp.ClientSession() as session:
        await publish_to_dlq(channel, msg, session)
    await msg.ack()

    # Verify message arrived in DLQ
    dlq_msg = await _consume_one(dlq, timeout=5.0)
    assert dlq_msg.body == test_body
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
        assert set(cb.keys()) == {"partition", "file_id", "status", "timestamp"}
        assert cb["partition"] == "user-dlq"
        assert cb["file_id"] == "doc-dlq"
        assert cb["status"] == "error"
        datetime.fromisoformat(cb["timestamp"])  # parsable ISO 8601
    finally:
        await server.close()


async def test_concurrent_messages_same_file(rmq_channel):
    """Publish 2 messages with the same file_id to the main queue.

    Both should be consumed without message loss. This is a basic race
    condition smoke test -- full concurrency testing is Phase 7 scope.
    """
    channel, main_q, _ = rmq_channel

    exchange = await channel.get_exchange(TEST_EXCHANGE)
    bodies = [b'{"file_id": "same-file", "seq": 1}', b'{"file_id": "same-file", "seq": 2}']

    for body in bodies:
        await exchange.publish(
            Message(
                body,
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key="test.retry.index",
        )

    # Consume both messages
    received = []
    for _ in range(2):
        msg = await _consume_one(main_q, timeout=5.0)
        received.append(msg.body)
        await msg.ack()

    # Verify both messages arrived (order may vary)
    assert set(received) == set(bodies)
