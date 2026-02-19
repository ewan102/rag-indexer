import asyncio
import os
import shutil
import subprocess

import pytest
import pytest_asyncio
import aio_pika

import rag_indexer.config as config
import rag_indexer.processing as processing
import rag_indexer.transport as transport
from rag_indexer.transport import declare_topology


# ---------- Docker availability ----------

def _docker_is_available() -> bool:
    """Check if the Docker CLI is available and the daemon is running."""
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _amqp_is_responsive(url: str) -> bool:
    """Try an actual AMQP connection to verify RabbitMQ is fully ready.

    A socket check is not sufficient because the TCP port may be open
    before RabbitMQ's AMQP protocol handler is initialized.
    """
    try:
        loop = asyncio.new_event_loop()
        try:
            conn = loop.run_until_complete(
                asyncio.wait_for(aio_pika.connect(url), timeout=3)
            )
            loop.run_until_complete(conn.close())
            return True
        finally:
            loop.close()
    except Exception:
        return False


# ---------- Auto-skip for broker-dependent tests ----------

@pytest.fixture(scope="session")
def require_broker():
    """Skip tests that need a real RabbitMQ broker when Docker is not available.

    NOT autouse -- applied explicitly via pytestmark in test modules that need
    a broker. RAG stub tests (test_rag_stub.py) don't need a broker.

    When Docker IS available, pytest-docker will start RabbitMQ automatically.
    When Docker is NOT available, tests are skipped gracefully.
    """
    if not _docker_is_available():
        pytest.skip("Docker not available -- skipping broker integration tests")


# ---------- pytest-docker fixtures ----------

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    """Return path to docker-compose.test.yml for pytest-docker."""
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.test.yml")


@pytest.fixture(scope="session")
def rabbitmq_url(docker_ip, docker_services):
    """Build AMQP URL from pytest-docker's dynamic port mapping.

    Waits for RabbitMQ to accept AMQP connections before returning.
    Uses a real AMQP connection check (not just TCP socket) to ensure
    the broker is fully initialized.
    """
    port = docker_services.port_for("rabbitmq", 5672)
    url = f"amqp://guest:guest@{docker_ip}:{port}/"
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=1.0,
        check=lambda: _amqp_is_responsive(url),
    )
    return url


# ---------- Test topology with short TTLs ----------

TEST_EXCHANGE = "test.retry.topic"
TEST_QUEUE = "test.retry.main.q"
TEST_DLQ = "test.retry.dlq"
TEST_ROUTING_KEY = "test.retry.*"
TEST_RETRY_QUEUES = [
    ("test.retry.1s.q", 1000),
    ("test.retry.2s.q", 2000),
    ("test.retry.3s.q", 3000),
]


@pytest_asyncio.fixture
async def rmq_channel(rabbitmq_url, monkeypatch):
    """Provide a channel with test topology (short TTLs) and clean queues.

    Monkeypatches config values in ALL modules that import them so
    declare_topology() and processing helpers use test-specific names.
    """
    # Monkeypatch config AND all modules that import from config.
    # Modules use `from config import X` which creates local bindings.
    # We must patch every module that holds a copy of these names.
    for mod in (config, transport):
        monkeypatch.setattr(mod, "EXCHANGE_NAME", TEST_EXCHANGE)
        monkeypatch.setattr(mod, "QUEUE_NAME", TEST_QUEUE)
        monkeypatch.setattr(mod, "DLQ_NAME", TEST_DLQ)
        monkeypatch.setattr(mod, "ROUTING_KEY", TEST_ROUTING_KEY)
        monkeypatch.setattr(mod, "RETRY_QUEUES", TEST_RETRY_QUEUES)
    # processing.py also imports RETRY_QUEUES
    monkeypatch.setattr(processing, "RETRY_QUEUES", TEST_RETRY_QUEUES)

    connection = await aio_pika.connect(rabbitmq_url)
    channel = await connection.channel()

    # Declare test topology (creates exchange, queues, bindings)
    main_q = await declare_topology(channel)

    # Purge all queues for clean state
    await main_q.purge()
    for qname, _ in TEST_RETRY_QUEUES:
        q = await channel.declare_queue(qname, passive=True)
        await q.purge()
    dlq = await channel.declare_queue(TEST_DLQ, passive=True)
    await dlq.purge()

    yield channel, main_q, dlq

    # Cleanup: delete test queues and exchange on a fresh channel.
    # Deleting a non-existent entity closes the channel, so we recover.
    cleanup_ch = await connection.channel()
    for name in [TEST_QUEUE, TEST_DLQ] + [q for q, _ in TEST_RETRY_QUEUES]:
        try:
            await cleanup_ch.queue_delete(name)
        except Exception:
            cleanup_ch = await connection.channel()
    try:
        await cleanup_ch.exchange_delete(TEST_EXCHANGE)
    except Exception:
        pass

    await connection.close()
