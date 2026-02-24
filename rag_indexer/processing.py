import asyncio

import aio_pika
import aiohttp
import structlog

from rag_indexer import rag_client
from rag_indexer.config import RETRY_QUEUES
from rag_indexer.models import IndexMessage, RagConn, ContentSpec
from rag_indexer.errors import TransientError, FatalError

log = structlog.get_logger()


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
                if not version_remote or (msg.version and version_remote != msg.version):
                    need_index = True
            elif resp.status == 404:
                need_index = True
                is_new = True
            else:
                raise FatalError(f"RAG GET {resp.status}: {resp.text}")

            if not need_index:
                return  # rien a faire

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
