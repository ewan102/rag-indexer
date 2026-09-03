import asyncio
import json
from dataclasses import dataclass
from typing import Any

import aiohttp
import structlog
from aiohttp import FormData

from rag_indexer.config import HTTP_TIMEOUT
from rag_indexer.models import IndexMessage, RagConn
from rag_indexer.errors import TransientError, FatalError

log = structlog.get_logger()


@dataclass
class RagResponse:
    status: int
    text: str = ""
    json_data: dict[str, Any] | None = None


async def rag_get_file(
    session: aiohttp.ClientSession, rag: RagConn, partition: str, file_id: str
) -> RagResponse:
    log.debug("rag_api_call", method="GET", endpoint="file")
    url = f"{rag.base_url}/partition/{partition}/file/{file_id}"
    async with session.get(
        url, headers={"Authorization": f"Bearer {rag.api_key}"}, timeout=HTTP_TIMEOUT
    ) as resp:
        text = await resp.text()
        json_data = None
        if resp.status == 200:
            try:
                json_data = json.loads(text)
            except ValueError:
                pass
        return RagResponse(status=resp.status, text=text, json_data=json_data)


async def rag_delete(
    session: aiohttp.ClientSession, rag: RagConn, partition: str, file_id: str
) -> RagResponse:
    log.debug("rag_api_call", method="DELETE", endpoint="file")
    url = f"{rag.base_url}/indexer/partition/{partition}/file/{file_id}"
    async with session.delete(
        url, headers={"Authorization": f"Bearer {rag.api_key}"}, timeout=HTTP_TIMEOUT
    ) as resp:
        text = await resp.text()
        return RagResponse(status=resp.status, text=text)


async def get_producer_file(session: aiohttp.ClientSession, msg: IndexMessage) -> bytes:
    # The file_url carries its own authentication via the secret in its path
    # (cozy-stack /files/downloads/<secret>/<filename>); no Authorization header.
    log.debug("file_download", source="file_url")
    try:
        async with session.get(msg.content.file_url, timeout=HTTP_TIMEOUT) as resp:
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


# cozy-stack's callback handler reads only metadata.doc_rev (web/ai/index_status.go).
# OpenRAG itself has no fixed field list: indexing_callback.py echoes every metadata
# key except its own server-computed ones (UPLOAD_METADATA_SERVER_KEYS in
# core/utils/conts.py), so its real success callback also carries md5sum and any
# app_metadata. This keeps our own DLQ failure callback to what cozy actually reads,
# plus datetime/doctype for readability.
_ECHOED_METADATA_FIELDS = ("doc_rev", "datetime", "doctype")


def metadata_dict(
    *,
    doc_rev: str | None,
    md5sum: str | None,
    datetime: str | None,
    doctype: str | None,
    app_metadata: dict | None,
) -> dict[str, Any]:
    """Metadata stored on the OpenRAG document.

    doc_rev is never filled in from another field: cozy-stack reads it as a CouchDB
    revision, and anything else has generation 0 there, which makes every later status
    look outdated and be dropped without a trace.
    """
    meta = {
        "md5sum": md5sum or "",
        "datetime": datetime or "",
        "doctype": doctype or "",
    }
    if isinstance(app_metadata, dict):
        meta |= app_metadata
    # Set last: the callbacks are ordered on this, application metadata must not move it.
    meta["doc_rev"] = doc_rev or ""
    return meta


def build_metadata(msg: IndexMessage) -> dict[str, Any]:
    return metadata_dict(
        doc_rev=msg.doc_rev,
        md5sum=msg.md5sum,
        datetime=msg.datetime,
        doctype=msg.doctype,
        app_metadata=msg.app_metadata,
    )


def callback_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """Narrow document metadata down to what our own DLQ failure callback carries."""
    return {field: meta.get(field) for field in _ECHOED_METADATA_FIELDS}


async def rag_upsert(
    session: aiohttp.ClientSession,
    msg: IndexMessage,
    is_new: bool
) -> None:

    form = FormData()
    rag = msg.rag

    filename = msg.name or f"{msg.file_id}.bin"

    if msg.content and msg.content.file_url:
        data_bytes = await get_producer_file(session, msg)
    else:
        raise FatalError("No content provided")

    content_type = msg.content_type or "application/octet-stream"
    form.add_field("file", data_bytes, filename=filename, content_type=content_type)

    meta = build_metadata(msg)

    form.add_field("metadata", json.dumps(meta))

    # Forward the cozy callback URL so OpenRAG can POST the async indexing status.
    if msg.callback_url:
        form.add_field("callback_url", msg.callback_url)
        log.debug("upsert_callback_url_forwarded", callback_url=msg.callback_url)
    else:
        log.debug("upsert_callback_url_absent")

    # Token for OpenRAG to sign the callback. Presence is logged, never the value.
    if msg.callback_token:
        form.add_field("callback_token", msg.callback_token)
        log.debug("upsert_callback_token_forwarded")
    else:
        log.debug("upsert_callback_token_absent")

    params = {}
    if msg.dir_id:
        params["parent_id"] = msg.dir_id
    if msg.name:
        params["name"] = msg.name
    if msg.md5sum:
        params["md5sum"] = msg.md5sum

    url_base = f"{rag.base_url}/indexer/partition/{msg.partition}/file/{msg.file_id}"

    method = "POST" if is_new else "PUT"
    headers = {"Authorization": f"Bearer {msg.rag.api_key}", "Origin": rag.base_url}

    log.debug("rag_api_call", method=method, endpoint="indexer")
    async with session.request(
        method,
        url_base,
        data=form,
        params=params,
        headers=headers,
        timeout=HTTP_TIMEOUT,
    ) as resp:
        # read response body to release the connection
        resp_text = await resp.text()
        if resp.status == 429 or resp.status >= 500:
            raise TransientError(f"RAG {method} {resp.status}: {resp_text}")
        if resp.status == 409:
            log.warning("rag_upsert_conflict", method=method, file_id=msg.file_id, partition=msg.partition)
            return
        if resp.status >= 400:
            raise FatalError(f"RAG {method} {resp.status}: {resp_text}")
        return
