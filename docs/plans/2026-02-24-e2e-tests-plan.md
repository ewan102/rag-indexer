# Automated E2E Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add automated E2E tests covering the full pipeline (RabbitMQ publish → consumer processes → RAG API called → state verified), with a stateful stub for CI and optional real OpenRAG for live testing.

**Architecture:** A stateful aiohttp TestServer mimics OpenRAG's API (stores files in a dict). Tests publish messages to a real RabbitMQ exchange, consume them, and call `process_message()` with a session pointing at the stub (or real OpenRAG via `--e2e-live`). A `--e2e-live` pytest flag switches the RAG target.

**Tech Stack:** pytest, pytest-asyncio, pytest-docker, aiohttp TestServer, aio-pika

---

### Task 1: Fix broken integration tests (test_rag_stub.py)

The Task 3 refactor changed `rag_get_file`/`rag_delete` to return `RagResponse` instead of `aiohttp.ClientResponse`. Three integration tests in `test_rag_stub.py` call `await resp.read()` on the result, which now fails.

**Files:**
- Modify: `tests/integration/test_rag_stub.py:57-131`

**Step 1: Fix test_rag_get_file_calls_correct_url**

Remove `await resp.read()` — `RagResponse` already has the data parsed. The test verifies the stub was called at the correct URL, which is captured in `call_log` regardless.

```python
async def test_rag_get_file_calls_correct_url(rag_stub):
    """rag_get_file makes GET /partition/{p}/file/{f} against the stub."""
    server, call_log, _ = rag_stub
    base_url = str(server.make_url(""))
    rag = RagConn(base_url=base_url, api_key="test-key")

    async with aiohttp.ClientSession() as session:
        resp = await rag_get_file(session, rag, "p1", "f1")

    assert len(call_log) == 1
    assert call_log[0]["method"] == "GET"
    assert call_log[0]["path"] == "/partition/p1/file/f1"
    assert resp.status == 200
```

**Step 2: Fix test_rag_delete_calls_correct_url**

Same pattern — remove `await resp.read()`:

```python
async def test_rag_delete_calls_correct_url(rag_stub):
    """rag_delete makes DELETE /indexer/partition/{p}/file/{f} against the stub."""
    server, call_log, _ = rag_stub
    base_url = str(server.make_url(""))
    rag = RagConn(base_url=base_url, api_key="test-key")

    async with aiohttp.ClientSession() as session:
        resp = await rag_delete(session, rag, "p1", "f1")

    assert len(call_log) == 1
    assert call_log[0]["method"] == "DELETE"
    assert call_log[0]["path"] == "/indexer/partition/p1/file/f1"
    assert resp.status == 200
```

**Step 3: Fix test_rag_get_file_sends_auth_header**

Same pattern:

```python
async def test_rag_get_file_sends_auth_header(rag_stub):
    """rag_get_file sends Authorization: Bearer <key> header."""
    server, call_log, _ = rag_stub
    base_url = str(server.make_url(""))
    api_key = "my-secret-api-key"
    rag = RagConn(base_url=base_url, api_key=api_key)

    async with aiohttp.ClientSession() as session:
        resp = await rag_get_file(session, rag, "p1", "f1")

    assert len(call_log) == 1
    auth_header = call_log[0]["headers"].get("Authorization")
    assert auth_header == f"Bearer {api_key}"
```

**Step 4: Run integration tests**

Run: `uv run pytest tests/integration/test_rag_stub.py -v`
Expected: 4 passed

**Step 5: Commit**

```bash
git add tests/integration/test_rag_stub.py
git commit -m "fix: update integration tests for RagResponse return type"
```

---

### Task 2: Create E2E test infrastructure

**Files:**
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/conftest.py`

**Step 1: Create test package**

Create empty `tests/e2e/__init__.py`.

**Step 2: Create conftest.py with --e2e-live flag and fixtures**

```python
"""E2E test fixtures: stateful RAG stub, RabbitMQ integration, --e2e-live flag."""

import asyncio
import json

import aio_pika
import aiohttp
import pytest
import pytest_asyncio
from aio_pika import ExchangeType, Message, DeliveryMode
from aiohttp import web
from aiohttp.test_utils import TestServer

from rag_indexer.processing import process_message
from rag_indexer.errors import TransientError, FatalError

from tests.integration.conftest import _docker_is_available, _amqp_is_responsive


# ---------- CLI option ----------

def pytest_addoption(parser):
    parser.addoption(
        "--e2e-live",
        action="store_true",
        default=False,
        help="Run E2E tests against real OpenRAG at localhost:8083",
    )


# ---------- Stateful RAG stub ----------

class RagStubState:
    """In-memory file store mimicking OpenRAG."""

    def __init__(self):
        self.files: dict[tuple[str, str], dict] = {}  # (partition, file_id) -> metadata
        self.call_log: list[dict] = []


def _make_rag_stub_app(state: RagStubState) -> web.Application:
    """Build an aiohttp app that mimics OpenRAG's indexing API."""

    async def get_file(request):
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        state.call_log.append({"method": "GET", "partition": partition, "file_id": file_id})

        key = (partition, file_id)
        if key not in state.files:
            return web.json_response(
                {"detail": f"'{file_id}' not found in partition '{partition}'"},
                status=404,
            )
        return web.json_response({"metadata": state.files[key]}, status=200)

    async def post_file(request):
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        state.call_log.append({"method": "POST", "partition": partition, "file_id": file_id})

        key = (partition, file_id)
        if key in state.files:
            return web.json_response(
                {"detail": f"File '{file_id}' already exists in partition {partition}"},
                status=409,
            )

        # Parse multipart to extract metadata
        metadata = await _parse_metadata(request)
        metadata.update({
            "partition": partition,
            "file_id": file_id,
            "filename": "uploaded.bin",
        })
        state.files[key] = metadata
        return web.json_response({"task_status_url": "/indexer/task/fake-task-id"}, status=201)

    async def put_file(request):
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        state.call_log.append({"method": "PUT", "partition": partition, "file_id": file_id})

        key = (partition, file_id)
        metadata = await _parse_metadata(request)
        metadata.update({
            "partition": partition,
            "file_id": file_id,
            "filename": "uploaded.bin",
        })
        state.files[key] = metadata
        return web.json_response({"task_status_url": "/indexer/task/fake-task-id"}, status=202)

    async def delete_file(request):
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        state.call_log.append({"method": "DELETE", "partition": partition, "file_id": file_id})

        key = (partition, file_id)
        if key not in state.files:
            return web.Response(status=404)
        del state.files[key]
        return web.Response(status=204)

    async def _parse_metadata(request) -> dict:
        """Extract metadata JSON from multipart form data."""
        try:
            reader = await request.multipart()
            metadata = {}
            async for part in reader:
                if part.name == "metadata":
                    raw = await part.read()
                    metadata = json.loads(raw)
                elif part.name == "file":
                    await part.read()  # consume file body
            return metadata
        except Exception:
            return {}

    app = web.Application()
    app.router.add_get("/partition/{partition}/file/{file_id}", get_file)
    app.router.add_post("/indexer/partition/{partition}/file/{file_id}", post_file)
    app.router.add_put("/indexer/partition/{partition}/file/{file_id}", put_file)
    app.router.add_delete("/indexer/partition/{partition}/file/{file_id}", delete_file)
    return app


@pytest_asyncio.fixture
async def rag_stub():
    """Stateful RAG stub server. Returns (server, state)."""
    state = RagStubState()
    app = _make_rag_stub_app(state)
    server = TestServer(app)
    await server.start_server()
    yield server, state
    await server.close()


# ---------- RAG base URL (stub or live) ----------

@pytest_asyncio.fixture
async def rag_base_url(request, rag_stub):
    """Return RAG base URL: stub by default, real OpenRAG with --e2e-live."""
    if request.config.getoption("--e2e-live"):
        base_url = "http://localhost:8083"
        # Create test partition if needed
        async with aiohttp.ClientSession() as session:
            await session.post(f"{base_url}/partition/e2e-test-auto")
        yield base_url
        # Cleanup: delete test files (best-effort)
        return
    server, _ = rag_stub
    yield str(server.make_url(""))


@pytest.fixture
def rag_state(request, rag_stub):
    """Access stub state for assertions. Only available without --e2e-live."""
    if request.config.getoption("--e2e-live"):
        pytest.skip("rag_state not available in live mode")
    _, state = rag_stub
    return state


# ---------- RabbitMQ fixtures ----------

E2E_EXCHANGE = "e2e.test.topic"
E2E_QUEUE = "e2e.test.q"
E2E_ROUTING_KEY = "e2e.test.*"

@pytest.fixture(scope="session")
def require_docker():
    if not _docker_is_available():
        pytest.skip("Docker not available -- skipping E2E tests")


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    import os
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.test.yml")


@pytest.fixture(scope="session")
def rabbitmq_url(docker_ip, docker_services):
    port = docker_services.port_for("rabbitmq", 5672)
    url = f"amqp://guest:guest@{docker_ip}:{port}/"
    docker_services.wait_until_responsive(
        timeout=60.0, pause=1.0,
        check=lambda: _amqp_is_responsive(url),
    )
    return url


@pytest_asyncio.fixture
async def rmq(require_docker, rabbitmq_url):
    """Provide RabbitMQ connection, exchange, and queue for E2E tests."""
    connection = await aio_pika.connect(rabbitmq_url)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(E2E_EXCHANGE, ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue(E2E_QUEUE, durable=True)
    await queue.bind(exchange, routing_key=E2E_ROUTING_KEY)
    await queue.purge()

    yield channel, exchange, queue

    # Cleanup
    try:
        cleanup_ch = await connection.channel()
        await cleanup_ch.queue_delete(E2E_QUEUE)
        await cleanup_ch.exchange_delete(E2E_EXCHANGE)
    except Exception:
        pass
    await connection.close()


# ---------- Helpers ----------

async def publish_msg(exchange, body: bytes, headers: dict):
    """Publish a message to the E2E exchange."""
    await exchange.publish(
        Message(
            body=body,
            headers=headers,
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key="e2e.test.file",
    )


async def consume_and_process(queue, rag_base_url: str):
    """Consume one message from queue and process it. Returns None on success, raises on error."""
    async with queue.iterator() as it:
        msg = await asyncio.wait_for(it.__anext__(), timeout=5.0)

    async with aiohttp.ClientSession() as session:
        # Override rag_base_url in the message headers
        patched_headers = dict(msg.headers or {})
        patched_headers["rag_base_url"] = rag_base_url
        patched_headers["rag_api_key"] = ""

        patched_msg = type("PatchedMsg", (), {
            "body": msg.body,
            "headers": patched_headers,
        })()

        await process_message(patched_msg, session)

    await msg.ack()
```

**Step 3: Run to verify conftest imports**

Run: `uv run python -c "import tests.e2e.conftest"`
Expected: No import errors

**Step 4: Commit**

```bash
git add tests/e2e/__init__.py tests/e2e/conftest.py
git commit -m "feat: add E2E test infrastructure with stateful RAG stub"
```

---

### Task 3: Write E2E tests — upsert, skip, update

**Files:**
- Create: `tests/e2e/test_e2e.py`

**Step 1: Write test_upsert_new_file**

```python
"""E2E tests: full pipeline through RabbitMQ → consumer → RAG API."""

import hashlib

import aiohttp
import pytest

from rag_indexer.errors import TransientError

from tests.e2e.conftest import publish_msg, consume_and_process

pytestmark = pytest.mark.usefixtures("require_docker")


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
    """Publish upsert for a new file → consumer POSTs to RAG → file stored."""
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
```

**Step 2: Write test_idempotent_skip**

```python
async def test_idempotent_skip(rmq, rag_base_url, rag_state):
    """Same file+md5 sent again → consumer skips (no PUT)."""
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
```

**Step 3: Write test_update_existing_file**

```python
async def test_update_existing_file(rmq, rag_base_url, rag_state):
    """Same file_id with different content → consumer PUTs update."""
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
```

**Step 4: Run tests**

Run: `uv run pytest tests/e2e/test_e2e.py -v -k "upsert or skip or update"`
Expected: 3 passed (or skipped if Docker unavailable)

**Step 5: Commit**

```bash
git add tests/e2e/test_e2e.py
git commit -m "feat: add E2E tests for upsert, idempotent skip, and update"
```

---

### Task 4: Write E2E tests — delete and transient error

**Files:**
- Modify: `tests/e2e/test_e2e.py`

**Step 1: Write test_delete_file**

```python
async def test_delete_file(rmq, rag_base_url):
    """Delete existing file → consumer DELETEs in RAG → file gone."""
    channel, exchange, queue = rmq
    headers = {
        "action": "delete",
        "partition": "e2e-test-auto",
        "file_id": "doc-1",
        "rag_base_url": rag_base_url,
        "rag_api_key": "",
    }

    await publish_msg(exchange, b"", headers)
    await consume_and_process(queue, rag_base_url)

    # Verify file is gone
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{rag_base_url}/partition/e2e-test-auto/file/doc-1") as resp:
            assert resp.status == 404
```

**Step 2: Write test_delete_nonexistent**

```python
async def test_delete_nonexistent(rmq, rag_base_url):
    """Delete non-existent file → 404 treated as success (idempotent)."""
    channel, exchange, queue = rmq
    headers = {
        "action": "delete",
        "partition": "e2e-test-auto",
        "file_id": "no-such-file",
        "rag_base_url": rag_base_url,
        "rag_api_key": "",
    }

    await publish_msg(exchange, b"", headers)
    # Should not raise — 404 is treated as success
    await consume_and_process(queue, rag_base_url)
```

**Step 3: Write test_transient_error**

```python
async def test_transient_error_on_unreachable_rag(rmq):
    """Unreachable RAG URL → TransientError raised."""
    channel, exchange, queue = rmq
    body = b"some data"
    headers = {
        "action": "upsert",
        "partition": "e2e-test-auto",
        "file_id": "fail-doc",
        "md5sum": _md5(body),
        "content_type": "text/plain",
        "rag_base_url": "http://localhost:1",  # unreachable
        "rag_api_key": "",
    }

    await publish_msg(exchange, body, headers)
    with pytest.raises(TransientError, match="[Nn]etwork error|[Cc]onnect"):
        await consume_and_process(queue, "http://localhost:1")
```

**Step 4: Run all E2E tests**

Run: `uv run pytest tests/e2e/ -v`
Expected: 6 passed (or skipped if Docker unavailable)

**Step 5: Run full test suite**

Run: `uv run pytest tests/unit/ tests/integration/test_rag_stub.py tests/e2e/ -v`
Expected: All pass

**Step 6: Commit**

```bash
git add tests/e2e/test_e2e.py
git commit -m "feat: add E2E tests for delete and transient error scenarios"
```
