import asyncio
import signal
import sys
import time

import aio_pika
import aiohttp
import structlog
from aiormq import AMQPConnectionError
from pydantic import ValidationError
from structlog.contextvars import clear_contextvars, bind_contextvars

from rag_indexer.config import RABBITMQ_URL, HTTP_TIMEOUT
from rag_indexer.errors import TransientError, FatalError
from rag_indexer.logging import setup_logging
from rag_indexer.metrics import MESSAGES_TOTAL, PROCESSING_DURATION
from rag_indexer.processing import process_message, get_retry_count, next_retry_queue
from rag_indexer.transport import (
    declare_topology,
    publish_to_retry,
    publish_to_dlq,
    start_health_server,
    shutdown_event,
    request_shutdown,
)


async def main():
    setup_logging()
    log = structlog.get_logger()

    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
    except AMQPConnectionError:
        log.error("rabbitmq_connection_failed")
        sys.exit(1)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, request_shutdown)
    loop.add_signal_handler(signal.SIGINT, request_shutdown)

    async with connection:
        log.info("rabbitmq_connected")
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await declare_topology(channel)

        health_runner = await start_health_server(connection)

        session_timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT + 5)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            try:
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        clear_contextvars()
                        headers = message.headers or {}
                        action = headers.get("action", "unknown")
                        partition = headers.get("partition", "unknown")
                        bind_contextvars(
                            file_id=headers.get("file_id", "unknown"),
                            partition=partition,
                            action=action,
                        )
                        log.info("message_received")

                        # Check shutdown BEFORE processing
                        if shutdown_event.is_set():
                            await message.nack(requeue=True)
                            log.warning("shutdown_nack_requeue")
                            break

                        async with message.process(ignore_processed=True, requeue=False):
                            retry_count = get_retry_count(message)
                            start_time = time.monotonic()

                            try:
                                await process_message(message, session)
                                # ACK implicit via context manager on success
                                duration = time.monotonic() - start_time
                                MESSAGES_TOTAL.labels(action=action, status="success", partition=partition).inc()
                                PROCESSING_DURATION.labels(action=action).observe(duration)
                                log.info("message_processed", status="success", duration_s=round(duration, 3))

                            except ValidationError as ve:
                                duration = time.monotonic() - start_time
                                MESSAGES_TOTAL.labels(action=action, status="dlq", partition=partition).inc()
                                PROCESSING_DURATION.labels(action=action).observe(duration)
                                log.error("message_invalid_payload", error=str(ve))
                                await publish_to_dlq(channel, message)

                            except TransientError as te:
                                duration = time.monotonic() - start_time
                                PROCESSING_DURATION.labels(action=action).observe(duration)
                                queue_name = next_retry_queue(retry_count)
                                if queue_name:
                                    MESSAGES_TOTAL.labels(action=action, status="retry", partition=partition).inc()
                                    log.warning("message_retry", attempt=retry_count + 1, error=str(te), queue=queue_name)
                                    await publish_to_retry(channel, message, queue_name)
                                else:
                                    MESSAGES_TOTAL.labels(action=action, status="dlq", partition=partition).inc()
                                    log.warning("message_dlq_exhausted", attempts=retry_count)
                                    await publish_to_dlq(channel, message)

                            except FatalError as fe:
                                duration = time.monotonic() - start_time
                                MESSAGES_TOTAL.labels(action=action, status="dlq", partition=partition).inc()
                                PROCESSING_DURATION.labels(action=action).observe(duration)
                                log.error("message_fatal", error=str(fe))
                                await publish_to_dlq(channel, message)
            finally:
                await health_runner.cleanup()
                log.info("shutdown_complete")
