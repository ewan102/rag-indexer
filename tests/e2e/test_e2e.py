"""E2E tests: full pipeline through RabbitMQ -> consumer -> RAG API."""

import hashlib

import aiohttp
import pytest

from rag_indexer.errors import TransientError

from tests.e2e.conftest import publish_msg, consume_and_process

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
