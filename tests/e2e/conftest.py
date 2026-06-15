import asyncio
import json
import os

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

# ---------- E2E namespace constants ----------

E2E_EXCHANGE = "e2e.test.topic"
E2E_QUEUE = "e2e.test.q"
E2E_ROUTING_KEY = "e2e.test.*"


# ---------- pytest CLI flag ----------

def pytest_addoption(parser):
    parser.addoption(
        "--e2e-live",
        action="store_true",
        default=False,
        help="Run E2E tests against a live OpenRAG at localhost:8083 instead of the stateful stub.",
    )


# ---------- Stateful RAG stub ----------

class RagStubState:
    """In-memory state backing the RAG stub server."""

    def __init__(self):
        self.files: dict[tuple[str, str], dict] = {}
        self.call_log: list[dict] = []


def _make_rag_stub_app(state: RagStubState) -> web.Application:
    """Build an aiohttp app that mimics the OpenRAG API."""

    async def get_file(request: web.Request) -> web.Response:
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        key = (partition, file_id)
        state.call_log.append({"method": "GET", "partition": partition, "file_id": file_id})
        if key in state.files:
            return web.json_response({"metadata": state.files[key]}, status=200)
        return web.json_response({"detail": "Not found"}, status=404)

    async def post_file(request: web.Request) -> web.Response:
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        key = (partition, file_id)
        log_entry = {"method": "POST", "partition": partition, "file_id": file_id}
        state.call_log.append(log_entry)
        if key in state.files:
            return web.json_response({"detail": "Already exists"}, status=409)
        metadata = {}
        reader = await request.multipart()
        async for part in reader:
            if part.name == "metadata":
                raw = await part.read(decode=True)
                metadata = json.loads(raw)
            elif part.name == "callback_url":
                log_entry["callback_url"] = (await part.read(decode=True)).decode()
            elif part.name == "file":
                # consume to avoid hanging
                await part.read(decode=False)
        state.files[key] = metadata
        return web.json_response(
            {"task_status_url": f"/tasks/{partition}/{file_id}"}, status=201
        )

    async def put_file(request: web.Request) -> web.Response:
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        key = (partition, file_id)
        state.call_log.append({"method": "PUT", "partition": partition, "file_id": file_id})
        metadata = {}
        reader = await request.multipart()
        async for part in reader:
            if part.name == "metadata":
                raw = await part.read(decode=True)
                metadata = json.loads(raw)
            elif part.name == "file":
                await part.read(decode=False)
        state.files[key] = metadata
        return web.json_response({}, status=202)

    async def delete_file(request: web.Request) -> web.Response:
        partition = request.match_info["partition"]
        file_id = request.match_info["file_id"]
        key = (partition, file_id)
        state.call_log.append({"method": "DELETE", "partition": partition, "file_id": file_id})
        if key not in state.files:
            return web.json_response({"detail": "Not found"}, status=404)
        del state.files[key]
        return web.Response(status=204)

    app = web.Application()
    app.router.add_get("/partition/{partition}/file/{file_id}", get_file)
    app.router.add_post("/indexer/partition/{partition}/file/{file_id}", post_file)
    app.router.add_put("/indexer/partition/{partition}/file/{file_id}", put_file)
    app.router.add_delete("/indexer/partition/{partition}/file/{file_id}", delete_file)
    return app


# ---------- Fixtures ----------

@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def rag_stub():
    """Start a stateful RAG stub server, yield (server, state), close on teardown.

    Module-scoped so that state persists across ordered tests (e.g. upsert
    followed by idempotent skip followed by update).
    """
    state = RagStubState()
    app = _make_rag_stub_app(state)
    server = TestServer(app)
    await server.start_server()
    yield server, state
    await server.close()


@pytest.fixture(scope="module")
def rag_base_url(request, rag_stub):
    """Return the RAG base URL — stub by default, live with ``--e2e-live``."""
    if request.config.getoption("--e2e-live"):
        # In live mode, create an auto partition via POST (best-effort).
        # The caller should have a running OpenRAG at localhost:8083.
        import urllib.request

        try:
            req = urllib.request.Request(
                "http://localhost:8083/partition/e2e-test-auto",
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass  # partition may already exist
        return "http://localhost:8083"
    server, _state = rag_stub
    return str(server.make_url(""))


@pytest.fixture(scope="module")
def rag_state(request, rag_stub):
    """Return the RagStubState for assertion; skip when running live."""
    if request.config.getoption("--e2e-live"):
        pytest.skip("rag_state not available in live mode")
    _server, state = rag_stub
    return state


@pytest.fixture(scope="session")
def require_docker():
    """Skip if Docker is not available."""
    if not _docker_is_available():
        pytest.skip("Docker not available -- skipping E2E tests that need Docker")


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    """Return path to docker-compose.test.yml for pytest-docker."""
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.test.yml")


@pytest.fixture(scope="session")
def rabbitmq_url(docker_ip, docker_services):
    """Build AMQP URL from pytest-docker dynamic port, wait for responsiveness."""
    port = docker_services.port_for("rabbitmq", 5672)
    url = f"amqp://guest:guest@{docker_ip}:{port}/"
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=1.0,
        check=lambda: _amqp_is_responsive(url),
    )
    return url


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def rmq(require_docker, rabbitmq_url):
    """Connect to RabbitMQ, declare E2E exchange/queue, purge, yield, cleanup."""
    connection = await aio_pika.connect(rabbitmq_url)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        E2E_EXCHANGE, ExchangeType.TOPIC, durable=False, auto_delete=True,
    )
    queue = await channel.declare_queue(
        E2E_QUEUE, durable=False, auto_delete=True,
    )
    await queue.bind(exchange, routing_key=E2E_ROUTING_KEY)
    await queue.purge()

    yield channel, exchange, queue

    # Cleanup
    cleanup_ch = await connection.channel()
    try:
        await cleanup_ch.queue_delete(E2E_QUEUE)
    except Exception:
        cleanup_ch = await connection.channel()
    try:
        await cleanup_ch.exchange_delete(E2E_EXCHANGE)
    except Exception:
        pass
    await connection.close()


# ---------- Helper functions ----------

async def publish_msg(exchange, body: bytes, headers: dict) -> None:
    """Publish a persistent message to the E2E routing key."""
    await exchange.publish(
        Message(
            body=body,
            headers=headers,
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key="e2e.test.file",
    )


async def consume_and_process(queue, rag_base_url: str) -> None:
    """Consume one message, override RAG URL, call process_message().

    Raises TransientError or FatalError on failure.
    """
    msg = await asyncio.wait_for(queue.get(no_ack=False), timeout=5)

    # Build a patched message that overrides rag_base_url and rag_api_key
    # in the headers so process_message() talks to our stub (or live).
    original_headers = dict(msg.headers or {})
    original_headers["rag_base_url"] = rag_base_url
    original_headers["rag_api_key"] = "e2e-test-key"

    # Create a thin wrapper that delegates to the original message
    # but returns patched headers.
    class _PatchedMessage:
        """Minimal duck-type of aio_pika.IncomingMessage with patched headers."""

        def __init__(self, original, patched_headers):
            self._original = original
            self.headers = patched_headers
            self.body = original.body

    patched = _PatchedMessage(msg, original_headers)

    try:
        async with aiohttp.ClientSession() as session:
            await process_message(patched, session)
    finally:
        await msg.ack()
