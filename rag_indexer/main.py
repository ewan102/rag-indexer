import asyncio
import signal
import sys

import aio_pika
import aiohttp
from aiormq import AMQPConnectionError
from pydantic import ValidationError

from rag_indexer.config import RABBITMQ_URL, HTTP_TIMEOUT
from rag_indexer.errors import TransientError, FatalError
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
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
    except AMQPConnectionError as e:
        print(f"Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(1)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, request_shutdown)
    loop.add_signal_handler(signal.SIGINT, request_shutdown)

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await declare_topology(channel)

        health_runner = await start_health_server(connection)

        session_timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT + 5)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            try:
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        # Check shutdown BEFORE processing
                        if shutdown_event.is_set():
                            await message.nack(requeue=True)
                            print("Shutdown: nacked and requeued in-flight message", file=sys.stderr)
                            break

                        async with message.process(ignore_processed=True, requeue=False):
                            retry_count = get_retry_count(message)

                            try:
                                await process_message(message, session)
                                # ACK implicit via context manager on success

                            except ValidationError as ve:
                                print(f"[FATAL] invalid payload: {ve}", file=sys.stderr)
                                await publish_to_dlq(channel, message)

                            except TransientError as te:
                                print(f"[RETRY] transient error (attempt {retry_count + 1}): {te}", file=sys.stderr)
                                queue_name = next_retry_queue(retry_count)
                                if queue_name:
                                    await publish_to_retry(channel, message, queue_name)
                                else:
                                    print(f"[DLQ] retries exhausted after {retry_count} attempts", file=sys.stderr)
                                    await publish_to_dlq(channel, message)

                            except FatalError as fe:
                                print(f"[FATAL] {fe}", file=sys.stderr)
                                await publish_to_dlq(channel, message)
            finally:
                await health_runner.cleanup()
                print("Shutdown complete", file=sys.stderr)
