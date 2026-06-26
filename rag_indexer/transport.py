import asyncio
from datetime import datetime, timezone

import aio_pika
import aiohttp
import structlog
from aio_pika import ExchangeType, Message, DeliveryMode
from aiohttp import web
from prometheus_client.aiohttp import make_aiohttp_handler

from rag_indexer.config import EXCHANGE_NAME, ROUTING_KEY, QUEUE_NAME, RETRY_QUEUES, DLQ_NAME
from rag_indexer.processing import extract_metadata

log = structlog.get_logger()


# ---------- Shutdown coordination ----------
shutdown_event = asyncio.Event()


def request_shutdown():
    """Signal handler -- plain function, NOT async. Safe for loop.add_signal_handler()."""
    log.warning("sigterm_received", detail="initiating graceful shutdown")
    shutdown_event.set()


# ---------- Health endpoint ----------
async def health_handler(request):
    """Minimal health check -- returns 200 if RabbitMQ connection is alive."""
    connection = request.app["rmq_connection"]
    if connection.is_closed:
        return web.json_response({"status": "unhealthy"}, status=503)
    return web.json_response({"status": "healthy"}, status=200)


async def start_health_server(connection, host="0.0.0.0", port=8080):
    """Start a non-blocking HTTP health server alongside the consumer loop."""
    app = web.Application()
    app["rmq_connection"] = connection
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", make_aiohttp_handler())
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


# ---------- Retry / DLQ publishing ----------
async def publish_to_retry(
    channel: aio_pika.Channel,
    original_msg: aio_pika.IncomingMessage,
    retry_queue_name: str,
) -> None:
    """Publish message copy to a retry delay queue via the default exchange."""
    await channel.default_exchange.publish(
        Message(
            original_msg.body,
            content_type=original_msg.content_type,
            headers={**(original_msg.headers or {})},
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=retry_queue_name,
    )


async def publish_to_dlq(
    channel: aio_pika.Channel,
    original_msg: aio_pika.IncomingMessage,
    session: aiohttp.ClientSession,
) -> None:
    """Publish message copy to the dead letter queue.

    After the message safely lands in the DLQ, best-effort notify the cozy callback
    (via callback_url from AMQP headers or cozy-json body) that indexing failed.
    """
    # Republish to the DLQ FIRST -- the message must reach the DLQ no matter what.
    # Use real AMQP headers (including any x-death entries) to preserve replay fidelity
    # and keep the original wire format intact for manual inspection / replaying.
    real_headers = original_msg.headers or {}
    await channel.default_exchange.publish(
        Message(
            original_msg.body,
            content_type=original_msg.content_type,
            headers={**real_headers},
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=DLQ_NAME,
    )

    # Best-effort failure callback to cozy-stack -- must never block or fail the DLQ publish.
    # Use extract_metadata so the callback_url is found regardless of wire format: in
    # "headers" format it lives in the AMQP headers; in "cozy-json" format it is in the body.
    metadata = extract_metadata(original_msg)
    callback_url = metadata.get("callback_url")
    if not callback_url:
        # Old messages / tests without a callback field: nothing to notify.
        log.debug("dlq_callback_skipped", reason="no_callback_url")
        return

    # publish_to_dlq is only the terminal failure path (retries exhausted), so the
    # status is always "error" -- the canonical vocabulary expected by cozy-stack's
    # SetRAGStatus. timestamp is captured at POST time, ISO 8601 UTC.
    payload = {
        "partition": metadata.get("partition"),
        "file_id": metadata.get("file_id"),
        "status": "error",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        async with session.post(callback_url, json=payload) as resp:
            resp.raise_for_status()
        log.info("dlq_callback_sent", callback_url=callback_url)
    except Exception as e:
        log.warning("dlq_callback_failed", callback_url=callback_url, error=str(e))


# ---------- Topology declaration ----------
async def declare_topology(channel: aio_pika.Channel) -> aio_pika.Queue:
    """Declare exchanges, queues, and bindings. Returns the main queue."""
    # Main exchange (topic, durable)
    main_ex = await channel.declare_exchange(
        EXCHANGE_NAME, ExchangeType.TOPIC, durable=True
    )

    # Main queue (QUORUM -- Raft replication for data safety)
    main_q = await channel.declare_queue(
        QUEUE_NAME,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    await main_q.bind(main_ex, routing_key=ROUTING_KEY)
    # Also receive messages returning from retry delay queues
    await main_q.bind(main_ex, routing_key="rag.index.retry")

    # Retry delay queues (CLASSIC -- no consumers, TTL only)
    for qname, ttl in RETRY_QUEUES:
        await channel.declare_queue(
            qname,
            durable=True,
            arguments={
                "x-message-ttl": ttl,
                "x-dead-letter-exchange": EXCHANGE_NAME,
                "x-dead-letter-routing-key": "rag.index.retry",
            },
        )

    # DLQ (QUORUM -- dead letters must never be lost)
    await channel.declare_queue(
        DLQ_NAME,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )

    return main_q
