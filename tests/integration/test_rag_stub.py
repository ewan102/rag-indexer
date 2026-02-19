"""Integration tests for rag_client functions against a real HTTP server.

Uses aiohttp's TestServer to spin up an in-process stub that records
requests and returns programmable responses. Does NOT require RabbitMQ.
"""

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer

from rag_indexer.models import IndexMessage, RagConn, ContentSpec
from rag_indexer.rag_client import rag_get_file, rag_delete, get_producer_file


# ---------- RAG stub fixture ----------

@pytest_asyncio.fixture
async def rag_stub():
    """In-process HTTP server that simulates the RAG API.

    Yields (server, call_log, response_overrides).
    - call_log: list of dicts recording each request
    - response_overrides: dict of (method, path) -> (status, payload)
      to program specific responses
    """
    call_log = []
    response_overrides = {}

    async def handler(request):
        body = await request.read()
        call_log.append({
            "method": request.method,
            "path": request.path,
            "headers": dict(request.headers),
            "body_len": len(body),
        })
        key = (request.method, request.path)
        if key in response_overrides:
            status, payload = response_overrides[key]
            if isinstance(payload, bytes):
                return web.Response(status=status, body=payload)
            return web.json_response(payload, status=status)
        return web.json_response({"status": "ok"}, status=200)

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    server = TestServer(app)
    await server.start_server()
    yield server, call_log, response_overrides
    await server.close()


# ---------- Tests ----------

async def test_rag_get_file_calls_correct_url(rag_stub):
    """rag_get_file makes GET /partition/{p}/file/{f} against the stub."""
    server, call_log, _ = rag_stub
    base_url = str(server.make_url(""))
    rag = RagConn(base_url=base_url, api_key="test-key")

    async with aiohttp.ClientSession() as session:
        resp = await rag_get_file(session, rag, "p1", "f1")
        # Read response to avoid unclosed resource warning
        await resp.read()

    assert len(call_log) == 1
    assert call_log[0]["method"] == "GET"
    assert call_log[0]["path"] == "/partition/p1/file/f1"


async def test_rag_delete_calls_correct_url(rag_stub):
    """rag_delete makes DELETE /indexer/partition/{p}/file/{f} against the stub."""
    server, call_log, _ = rag_stub
    base_url = str(server.make_url(""))
    rag = RagConn(base_url=base_url, api_key="test-key")

    async with aiohttp.ClientSession() as session:
        resp = await rag_delete(session, rag, "p1", "f1")
        await resp.read()

    assert len(call_log) == 1
    assert call_log[0]["method"] == "DELETE"
    assert call_log[0]["path"] == "/indexer/partition/p1/file/f1"


async def test_get_producer_file_reads_content_via_stub(rag_stub):
    """get_producer_file reads binary content from the stub HTTP server.

    This validates that the async context manager correctly reads the
    response body from a real HTTP server (TEST-03 integration complement).
    """
    server, call_log, response_overrides = rag_stub
    expected_content = b"binary-file-content-here-\x00\x01\x02"
    file_path = "/files/doc.pdf"
    response_overrides[("GET", file_path)] = (200, expected_content)

    file_url = str(server.make_url(file_path))
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://unused", api_key="unused"),
        content=ContentSpec(file_url=file_url),
    )

    async with aiohttp.ClientSession() as session:
        result = await get_producer_file(session, msg)

    assert result == expected_content
    assert len(call_log) == 1
    assert call_log[0]["method"] == "GET"
    assert call_log[0]["path"] == file_path


async def test_rag_get_file_sends_auth_header(rag_stub):
    """rag_get_file sends Authorization: Bearer <key> header."""
    server, call_log, _ = rag_stub
    base_url = str(server.make_url(""))
    api_key = "my-secret-api-key"
    rag = RagConn(base_url=base_url, api_key=api_key)

    async with aiohttp.ClientSession() as session:
        resp = await rag_get_file(session, rag, "p1", "f1")
        await resp.read()

    assert len(call_log) == 1
    auth_header = call_log[0]["headers"].get("Authorization")
    assert auth_header == f"Bearer {api_key}"
