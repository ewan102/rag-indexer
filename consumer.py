import asyncio
import json
import os
import sys
from typing import Optional, Dict, Any

import aiohttp
import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aiormq import AMQPConnectionError
from pydantic import BaseModel, Field, ValidationError
from aiohttp import FormData

# ---------- Error hierarchy ----------
class TransientError(Exception):
    """Retryable failure. Consumer will route to the appropriate retry delay queue."""


class FatalError(Exception):
    """Non-retryable failure. Consumer will route directly to DLQ."""


# ---------- Config via env ----------
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "rag.index.topic")
ROUTING_KEY = os.getenv("ROUTING_KEY", "rag.index.*")
QUEUE_NAME = os.getenv("QUEUE_NAME", "rag.index.q")

RETRY_QUEUES = [
    ("rag.index.retry.30s.q", 30_000),
    ("rag.index.retry.5m.q", 300_000),
    ("rag.index.retry.1h.q", 3_600_000),
]
MAX_RETRIES = int(os.getenv("MAX_RETRIES", str(len(RETRY_QUEUES))))
DLQ_NAME = os.getenv("DLQ_NAME", "rag.index.dlq")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))


# ---------- Message schema ----------
class RagConn(BaseModel):
    base_url: str
    api_key: str


class ContentSpec(BaseModel):
    note_markdown: Optional[str] = None
    file_url: Optional[str] = None
    file_bearer: Optional[str] = None


class IndexMessage(BaseModel):
    action: str  # "upsert" | "delete"
    partition: str
    file_id: str
    doctype: Optional[str] = None
    version: Optional[str] = None
    md5sum: Optional[str] = None
    name: Optional[str] = None
    dir_id: Optional[str] = None
    datetime: Optional[str] = None
    content_type: Optional[str] = None
    app_metadata: Optional[dict] = None
    rag: RagConn
    content: Optional[ContentSpec] = None


# ---------- Helpers ----------
def get_retry_count(message: aio_pika.IncomingMessage) -> int:
    """Count completed retry cycles from x-death header entries.

    Each dead-letter cycle through a TTL retry queue adds an x-death entry
    with reason='expired'. The count of such entries equals the number of
    completed retry cycles.
    """
    x_death = (message.headers or {}).get("x-death")
    if not x_death:
        return 0
    return sum(1 for entry in x_death if entry.get("reason") == "expired")


def next_retry_queue(retry_count: int) -> str | None:
    """Return the queue name for the given retry stage, or None if exhausted."""
    if retry_count < len(RETRY_QUEUES):
        return RETRY_QUEUES[retry_count][0]
    return None


# ---------- HTTP calls to RAG ----------
async def rag_get_file(
    session: aiohttp.ClientSession, rag: RagConn, partition: str, file_id: str
) -> aiohttp.ClientResponse:
    url = f"{rag.base_url}/partition/{partition}/file/{file_id}"
    return await session.get(
        url, headers={"Authorization": f"Bearer {rag.api_key}"}, timeout=HTTP_TIMEOUT
    )


async def rag_delete(
    session: aiohttp.ClientSession, rag: RagConn, partition: str, file_id: str
) -> aiohttp.ClientResponse:
    url = f"{rag.base_url}/indexer/partition/{partition}/file/{file_id}"
    return await session.delete(
        url, headers={"Authorization": f"Bearer {rag.api_key}"}, timeout=HTTP_TIMEOUT
    )


async def get_producer_file(session: aiohttp.ClientSession, msg: IndexMessage) -> bytes:
    headers = {}
    if msg.content.file_bearer:
        headers["Authorization"] = f"Bearer {msg.content.file_bearer}"

    try:
        async with session.get(msg.content.file_url, headers=headers, timeout=HTTP_TIMEOUT) as resp:
            if resp.status == 429 or resp.status >= 500:
                raise TransientError(f"Failed to fetch file_url ({resp.status})")
            if resp.status >= 400:
                raise FatalError(f"Failed to fetch file_url ({resp.status})")
            file = await resp.read()
        return file
    except TransientError:
        raise
    except FatalError:
        raise
    except asyncio.TimeoutError as e:
        raise TransientError(f"Timeout fetching file_url: {e}") from e
    except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as e:
        raise TransientError(f"Network error fetching file_url: {e}") from e

def build_metadata(msg: IndexMessage) -> Dict[str, Any]:
    meta = {
        "version": msg.version or msg.md5sum or "",
        "datetime": msg.datetime or "",
        "doctype": msg.doctype or "",
    }
    if msg.app_metadata is not None and isinstance(msg.app_metadata, dict):
        # Custom metadata from app
        app_meta = msg.app_metadata
        meta |= app_meta
    return meta


async def rag_upsert(
    session: aiohttp.ClientSession,
    msg: IndexMessage,
    file: bytes,
    is_new: bool
) -> aiohttp.ClientResponse:

    form = FormData()
    rag = msg.rag

    # Content source: note_markdown OR file_url download
    filename = msg.name or f"{msg.file_id}.bin"

    if file is not None:
        data_bytes = file
    elif msg.content and msg.content.file_url:
        data_bytes = await get_producer_file(session, msg)
    else:
        raise FatalError("No content provided")

    content_type = msg.content_type or "application/octet-stream"
    form.add_field("file", data_bytes, filename=filename, content_type=content_type)

    meta = build_metadata(msg)

    form.add_field("metadata", json.dumps(meta))

    # query params. TODO: check it is useful, should not
    params = {}
    if msg.dir_id:
        params["parent_id"] = msg.dir_id
    if msg.name:
        params["name"] = msg.name
    if msg.md5sum:
        params["md5sum"] = msg.md5sum

    # POST (new) vs PUT (update) est décidé après GET d’exist. ci-dessous
    # Ici on ne choisit pas encore l’URL exacte; on la construit dans le flow principal
    url_base = f"{rag.base_url}/indexer/partition/{msg.partition}/file/{msg.file_id}"

    method = "POST" if is_new else "PUT"
    headers = {"Authorization": f"Bearer {msg.rag.api_key}"}

    async with session.request(
        method,
        url_base,
        data=form,
        params=params,
        headers=headers,
        timeout=HTTP_TIMEOUT,
    ) as resp:
        # lecture pour vider le flux (évite connexion occupée)
        resp_text = await resp.text()
        if resp.status == 429 or resp.status >= 500:
            raise TransientError(f"RAG {method} {resp.status}: {resp_text}")
        if resp.status >= 400:
            raise FatalError(f"RAG {method} {resp.status}: {resp_text}")
        return


# ---------- Processing ----------
async def process_message(
    message: aio_pika.IncomingMessage, session: aiohttp.ClientSession
) -> None:

    try:
        body_bytes = message.body
        headers = message.headers

        # payload = json.loads(body.decode("utf-8"))
        # msg = IndexMessage.model_validate(payload)

        msg = IndexMessage(
            action=headers.get("action", "upsert"),
            partition=headers.get("partition") or "",
            file_id=headers.get("file_id"),
            doctype=headers.get("doctype"),
            version=headers.get("version"),
            name=headers.get("name"),
            dir_id=headers.get("dir_id"),
            datetime=headers.get("datetime"),
            content_type=headers.get("content_type"),
            rag=RagConn(
                base_url=headers.get("rag_base_url", ""),
                api_key=headers.get("rag_api_key", ""),
            ),
            # content.file reste None: le binaire est dans `body`
            content=ContentSpec(
                file_url=headers.get("file_url"),
                file_bearer=headers.get("file_bearer"),
            ),
        )
        print("process new message", msg)
        # DELETE path
        if msg.action == "delete":
            resp = await rag_delete(session, msg.rag, msg.partition, msg.file_id)
            # 2xx or 404 -> OK (idempotent)
            if 200 <= resp.status < 300 or resp.status == 404:
                await resp.read()
                return
            # 429 or 5xx -> retry
            text = await resp.text()
            if resp.status == 429 or resp.status >= 500:
                raise TransientError(f"RAG delete {resp.status}: {text}")
            # other 4xx -> non-retry (e.g. 401/403 -> config)
            raise FatalError(f"RAG delete {resp.status}: {text}")

        # UPSERT path
        if msg.action == "upsert":
            # 1) GET current
            print("go get file")
            resp = await rag_get_file(session, msg.rag, msg.partition, msg.file_id)
            if resp.status == 429 or resp.status >= 500:
                print("RAG GET error", resp.status)
                text = await resp.text()
                raise TransientError(f"RAG GET {resp.status}: {text}")

            need_index = False
            is_new = False
            if resp.status == 200:
                doc = await resp.json()
                doc_metadata = (doc.get("metadata") or {}) if isinstance(doc, dict) else {}
                version_remote = doc_metadata.get("version") or doc_metadata.get("md5sum") # retro compat
                if not version_remote or (msg.version and version_remote != msg.version):
                    need_index = True
            elif resp.status == 404:
                need_index = True
                is_new = True
            else:
                # other 4xx -> non-retry (bad auth/config)
                text = await resp.text()
                raise FatalError(f"RAG GET {resp.status}: {text}")

            if not need_index:
                return  # rien à faire

            # 2) Build multipart & send
            return await rag_upsert(session, msg, body_bytes, is_new)
        else:
            raise FatalError(f"Unknown action: {msg.action}")

    except TransientError:
        raise
    except FatalError:
        raise
    except asyncio.TimeoutError as e:
        raise TransientError(f"Timeout: {e}") from e
    except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as e:
        raise TransientError(f"Network error: {e}") from e
    except Exception as e:
        # Unexpected error -- treat as fatal to avoid infinite retry
        raise FatalError(f"Unexpected error: {e}") from e


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
) -> None:
    """Publish message copy to the dead letter queue."""
    await channel.default_exchange.publish(
        Message(
            original_msg.body,
            content_type=original_msg.content_type,
            headers={**(original_msg.headers or {})},
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=DLQ_NAME,
    )


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


# ---------- Consumer loop ----------
async def main():
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
    except AMQPConnectionError as e:
        print(f"Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(1)

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await declare_topology(channel)

        session_timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT + 5)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
