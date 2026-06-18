import asyncio
import json

import aio_pika
import aiohttp
import structlog

from rag_indexer import rag_client
from rag_indexer.config import RETRY_QUEUES
from rag_indexer.models import IndexMessage, RagConn, ContentSpec
from rag_indexer.errors import TransientError, FatalError

log = structlog.get_logger()

# Two wire formats coexist:
#   "headers" format   — all business fields are AMQP message headers (legacy, used by all
#                        existing producers and tests). Body contains binary file content.
#   "cozy-json" format — AMQP headers carry only broker-added fields (x-death, …); all
#                        business fields are JSON-encoded in the body. File content is always
#                        fetched via file_url. Used when the producer cannot set custom AMQP
#                        headers (e.g. cozy-stack's shared rabbitmq package).
#
# The two formats are transparent to all code above extract_metadata().


def extract_metadata(message: aio_pika.IncomingMessage) -> dict:
    """Return business metadata fields from a message regardless of wire format.

    Detection heuristic:
    - If 'partition' is present in message.headers → "headers" format, return headers.
    - Else if body is valid JSON containing 'partition' → "cozy-json" format, return body dict.
    - Else → return headers as-is (fallback; downstream validation will reject the message).

    Why 'partition' rather than checking for empty headers:
        RabbitMQ appends x-death, x-first-death-reason, x-first-death-queue, and
        x-first-death-exchange headers during dead-lettering through TTL retry queues. A
        cozy-json message that has completed a retry cycle will arrive with non-empty AMQP
        headers containing only these broker-added fields — no business fields. Checking for
        the presence of the required business field 'partition' is robust against this.
    """
    raw_headers = message.headers or {}
    if "partition" in raw_headers:
        return raw_headers  # "headers" format

    try:
        body = message.body
        if body:
            data = json.loads(body.decode("utf-8"))
            if isinstance(data, dict) and "partition" in data:
                return data
    except Exception:
        pass

    return raw_headers  # fallback: downstream validation will reject this


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


async def process_message(
    message: aio_pika.IncomingMessage, session: aiohttp.ClientSession
) -> None:
    """Validate, route, and execute the indexing action for one message.

    Supports both 'headers' and 'cozy-json' wire formats via extract_metadata().
    Raises TransientError for retryable failures and FatalError for permanent ones;
    unexpected exceptions are wrapped as FatalError to prevent infinite retry loops.
    """
    try:
        headers = extract_metadata(message)
        # For cozy-json format the body is the metadata JSON, not file content.
        # Pass None so rag_upsert falls through to the file_url download path.
        # For headers format the body is binary file content (or b"" for URL-based upserts).
        body_bytes = message.body if "partition" in (message.headers or {}) else None

        msg = IndexMessage(
            action=headers.get("action", "upsert"),
            partition=headers.get("partition") or "",
            file_id=headers.get("file_id"),
            doctype=headers.get("doctype"),
            version=headers.get("version"),
            md5sum=headers.get("md5sum"),
            name=headers.get("name"),
            dir_id=headers.get("dir_id"),
            datetime=headers.get("datetime"),
            content_type=headers.get("content_type"),
            callback_url=headers.get("callback_url"),
            rag=RagConn(
                base_url=headers.get("rag_base_url", ""),
                api_key=headers.get("rag_api_key", ""),
            ),
            content=ContentSpec(
                file_url=headers.get("file_url"),
                file_bearer=headers.get("file_bearer"),
            ),
        )
        log.debug("message_processing")
        # DELETE path
        if msg.action == "delete":
            resp = await rag_client.rag_delete(session, msg.rag, msg.partition, msg.file_id)
            if 200 <= resp.status < 300 or resp.status == 404:
                return
            if resp.status == 429 or resp.status >= 500:
                raise TransientError(f"RAG delete {resp.status}: {resp.text}")
            raise FatalError(f"RAG delete {resp.status}: {resp.text}")

        # UPSERT path
        if msg.action == "upsert":
            # 1) GET current
            log.debug("rag_get_file", detail="checking current version")
            resp = await rag_client.rag_get_file(session, msg.rag, msg.partition, msg.file_id)
            if resp.status == 429 or resp.status >= 500:
                log.debug("rag_get_error", status=resp.status)
                raise TransientError(f"RAG GET {resp.status}: {resp.text}")

            need_index = False
            is_new = False
            if resp.status == 200:
                doc = resp.json_data or {}
                doc_metadata = (doc.get("metadata") or {}) if isinstance(doc, dict) else {}
                version_remote = doc_metadata.get("version") or doc_metadata.get("md5sum")  # retro compat
                version_local = msg.version or msg.md5sum
                if not version_remote or (version_local and version_remote != version_local):
                    need_index = True
            elif resp.status == 404:
                need_index = True
                is_new = True
            else:
                raise FatalError(f"RAG GET {resp.status}: {resp.text}")

            if not need_index:
                return  # nothing to do

            # 2) Build multipart & send
            return await rag_client.rag_upsert(session, msg, body_bytes, is_new)
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
