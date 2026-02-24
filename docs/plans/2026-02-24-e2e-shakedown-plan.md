# E2E Shakedown Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix bugs found during code review and validate the full pipeline end-to-end against real RabbitMQ + OpenRAG services.

**Architecture:** Bug fixes are isolated to `config.py`, `rag_client.py`, and `processing.py`. E2E testing uses existing `scripts/producer.py` to publish messages and a docker-compose file for RabbitMQ. OpenRAG is already running on localhost:8083.

**Tech Stack:** Python 3.12, aio-pika, aiohttp, pydantic, structlog, Docker, RabbitMQ

---

### Task 1: Add dotenv loading to config.py

**Files:**
- Modify: `rag_indexer/config.py:1-3`

**Step 1: Add load_dotenv at the top of config.py**

Add `from dotenv import load_dotenv` and call `load_dotenv()` before any `os.getenv()` call. This ensures `.env` is loaded when the consumer starts.

```python
import os

from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
# ... rest unchanged
```

**Step 2: Run existing tests to verify no regressions**

Run: `uv run pytest tests/unit/ -v`
Expected: All 39 tests pass (dotenv loading is a no-op when `.env` is missing in CI)

**Step 3: Commit**

```bash
git add rag_indexer/config.py
git commit -m "fix: load .env file in consumer config"
```

---

### Task 2: Fix aiohttp timeout type

**Files:**
- Modify: `rag_indexer/config.py:54`
- Modify: `rag_indexer/rag_client.py:9,22-23,31-32,43,117-123`

**Step 1: Create a proper ClientTimeout in config.py**

Replace the raw int with an `aiohttp.ClientTimeout`:

```python
import aiohttp

_HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT", "60"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_SECONDS)
```

**Step 2: Update main.py session creation**

`main.py:51` currently does `aiohttp.ClientTimeout(total=HTTP_TIMEOUT + 5)`. Since `HTTP_TIMEOUT` is now a `ClientTimeout` object, update to use the raw seconds value:

```python
session_timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_SECONDS + 5)
```

Import `_HTTP_TIMEOUT_SECONDS` from config, or just use `HTTP_TIMEOUT` (the ClientTimeout) directly for the session and remove the `+5` since per-request timeouts in rag_client.py will use the exact value.

Simplest approach: use `HTTP_TIMEOUT` (the ClientTimeout) everywhere. Remove the session-level `+5` buffer since it was a workaround for the type mismatch.

In `main.py`:
```python
from rag_indexer.config import RABBITMQ_URL, HTTP_TIMEOUT, CONCURRENCY

# line 51-52 becomes:
session = aiohttp.ClientSession(timeout=HTTP_TIMEOUT)
```

**Step 3: Run existing tests**

Run: `uv run pytest tests/unit/ -v`
Expected: All pass (tests monkeypatch rag_client functions, so timeout type doesn't matter in tests)

**Step 4: Commit**

```bash
git add rag_indexer/config.py rag_indexer/main.py
git commit -m "fix: use aiohttp.ClientTimeout instead of raw int"
```

---

### Task 3: Fix connection leaks in rag_get_file and rag_delete

**Files:**
- Modify: `rag_indexer/rag_client.py:16-33`
- Modify: `rag_indexer/processing.py:68-105`
- Modify: `tests/conftest.py` (update FakeResp to support `async with`)
- Modify: `tests/unit/test_processing.py` (update fakes)

**Context:** Currently `rag_get_file` and `rag_delete` return raw `aiohttp.ClientResponse` objects without using `async with`. The caller reads the body but the connection is never properly released. The cleanest fix: make these functions use `async with` internally and return a simple dataclass with the data already parsed.

**Step 1: Create RagResponse dataclass in rag_client.py**

Add at the top of `rag_client.py`:

```python
from dataclasses import dataclass, field
from typing import Any

@dataclass
class RagResponse:
    status: int
    text: str = ""
    json_data: dict[str, Any] | None = None
```

**Step 2: Refactor rag_get_file to return RagResponse**

```python
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
            import json as _json
            try:
                json_data = _json.loads(text)
            except ValueError:
                pass
        return RagResponse(status=resp.status, text=text, json_data=json_data)
```

**Step 3: Refactor rag_delete to return RagResponse**

```python
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
```

**Step 4: Update processing.py DELETE path**

In `processing.py`, the delete path (lines 68-79) currently does:
```python
resp = await rag_client.rag_delete(session, msg.rag, msg.partition, msg.file_id)
if 200 <= resp.status < 300 or resp.status == 404:
    await resp.read()
    return
text = await resp.text()
```

Replace with:
```python
resp = await rag_client.rag_delete(session, msg.rag, msg.partition, msg.file_id)
if 200 <= resp.status < 300 or resp.status == 404:
    return
if resp.status == 429 or resp.status >= 500:
    raise TransientError(f"RAG delete {resp.status}: {resp.text}")
raise FatalError(f"RAG delete {resp.status}: {resp.text}")
```

**Step 5: Update processing.py UPSERT GET path**

Lines 84-105 currently use `resp.text()`, `resp.json()`, `resp.status`. Replace with:

```python
log.debug("rag_get_file", detail="checking current version")
resp = await rag_client.rag_get_file(session, msg.rag, msg.partition, msg.file_id)
if resp.status == 429 or resp.status >= 500:
    log.debug("rag_get_error", status=resp.status)
    raise TransientError(f"RAG GET {resp.status}: {resp.text}")

need_index = False
is_new = False
if resp.status == 200:
    doc = resp.json_data or {}
    doc_metadata = doc.get("metadata") or {} if isinstance(doc, dict) else {}
    version_remote = doc_metadata.get("version") or doc_metadata.get("md5sum")
    if not version_remote or (msg.version and version_remote != msg.version):
        need_index = True
elif resp.status == 404:
    need_index = True
    is_new = True
else:
    raise FatalError(f"RAG GET {resp.status}: {resp.text}")
```

**Step 6: Update test fakes**

The tests monkeypatch `rag_client.rag_get_file` and `rag_client.rag_delete` with fake async functions that return `FakeResp`. Since tests monkeypatch at the function level, the fake functions don't need to change — they just need to return objects with `.status`, `.text`, and `.json_data` attributes matching `RagResponse`.

Update `FakeResp` in `tests/conftest.py`:

```python
class FakeResp:
    """Mimics RagResponse returned by rag_get_file / rag_delete."""
    def __init__(self, status: int, json_data=None, text_data=""):
        self.status = status
        self.json_data = json_data
        self.text = text_data

    # Keep async methods for backward compat with any test that calls them directly
    async def json(self):
        return self.json_data

    async def read(self):
        return b""
```

Update test assertions in `test_processing.py` — the tests that check `resp.text` vs calling `await resp.text()` should still work since the monkeypatched fakes return the object with `.text` as attribute now.

**Step 7: Run tests**

Run: `uv run pytest tests/unit/ -v`
Expected: All pass

**Step 8: Commit**

```bash
git add rag_indexer/rag_client.py rag_indexer/processing.py tests/conftest.py tests/unit/test_processing.py
git commit -m "fix: use async context managers in rag_client to prevent connection leaks"
```

---

### Task 4: Handle 409 Conflict in rag_upsert

**Files:**
- Modify: `rag_indexer/rag_client.py:127-130`

**Step 1: Write failing test**

In `tests/unit/test_processing.py`, add a test for 409 on POST being retried:

```python
@pytest.mark.asyncio
async def test_upsert_post_409_raises_transient_error(
    monkeypatch, headers_base, aiohttp_session_stub
):
    """POST returning 409 Conflict (race condition) should be TransientError, not FatalError."""
    async def fake_get(session, rag, partition, file_id):
        return FakeResp(404)  # file doesn't exist

    async def fake_upsert(session, msg, file_bytes, is_new):
        # Simulate 409 by raising TransientError (what rag_upsert should do)
        from rag_indexer.errors import TransientError
        raise TransientError("RAG POST 409: File already exists")

    monkeypatch.setattr(rag_client, "rag_get_file", fake_get)
    monkeypatch.setattr(rag_client, "rag_upsert", fake_upsert)

    headers = {**headers_base, "action": "upsert"}
    msg = DummyMessage(body=b"data", headers=headers)
    with pytest.raises(TransientError, match="409"):
        await process_message(msg, aiohttp_session_stub)
```

**Step 2: Run test to verify it passes (the error propagation already works)**

Run: `uv run pytest tests/unit/test_processing.py::test_upsert_post_409_raises_transient_error -v`
Expected: PASS (the test verifies the contract)

**Step 3: Fix rag_upsert to treat 409 as TransientError**

In `rag_client.py`, the current error handling (lines 127-130):

```python
if resp.status == 429 or resp.status >= 500:
    raise TransientError(f"RAG {method} {resp.status}: {resp_text}")
if resp.status >= 400:
    raise FatalError(f"RAG {method} {resp.status}: {resp_text}")
```

Change to:

```python
if resp.status == 429 or resp.status >= 500:
    raise TransientError(f"RAG {method} {resp.status}: {resp_text}")
if resp.status == 409:
    raise TransientError(f"RAG {method} {resp.status} Conflict: {resp_text}")
if resp.status >= 400:
    raise FatalError(f"RAG {method} {resp.status}: {resp_text}")
```

**Step 4: Run all tests**

Run: `uv run pytest tests/unit/ -v`
Expected: All pass

**Step 5: Commit**

```bash
git add rag_indexer/rag_client.py tests/unit/test_processing.py
git commit -m "fix: treat 409 Conflict as TransientError for retry"
```

---

### Task 5: Fix rag_upsert return type annotation

**Files:**
- Modify: `rag_indexer/rag_client.py:73-78`

**Step 1: Fix the return type**

Change the function signature from:

```python
async def rag_upsert(
    session: aiohttp.ClientSession,
    msg: IndexMessage,
    file: bytes,
    is_new: bool
) -> aiohttp.ClientResponse:
```

To:

```python
async def rag_upsert(
    session: aiohttp.ClientSession,
    msg: IndexMessage,
    file: bytes,
    is_new: bool
) -> None:
```

**Step 2: Run tests**

Run: `uv run pytest tests/unit/ -v`
Expected: All pass

**Step 3: Commit**

```bash
git add rag_indexer/rag_client.py
git commit -m "fix: correct rag_upsert return type annotation to None"
```

---

### Task 6: Create docker-compose for RabbitMQ E2E testing

**Files:**
- Create: `docker-compose.yml`

**Step 1: Create docker-compose.yml**

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 10s
      retries: 5
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
```

**Step 2: Start RabbitMQ**

Run: `docker compose up -d`
Wait: `docker compose exec rabbitmq rabbitmq-diagnostics -q ping` returns OK

**Step 3: Verify management UI accessible**

Open: http://localhost:15672 (guest/guest)

**Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "infra: add docker-compose for local RabbitMQ"
```

---

### Task 7: E2E — Create partition in OpenRAG

Before indexing, we need a partition to exist in OpenRAG.

**Step 1: Check OpenRAG health**

Run: `curl -s http://localhost:8083/health_check`
Expected: `"RAG API is up."`

**Step 2: Create a test partition**

Run: `curl -s -X POST http://localhost:8083/partition/e2e-test`
Expected: 200/201 with partition created (or 409 if already exists — that's fine)

**Step 3: Verify partition exists**

Run: `curl -s http://localhost:8083/partition/e2e-test`
Expected: 200 with partition details

---

### Task 8: E2E — Happy path upsert

**Step 1: Create a test file**

```bash
echo "Hello, this is a test document for RAG indexing." > /tmp/e2e-test.txt
```

**Step 2: Publish upsert message via producer**

```bash
uv run python scripts/producer.py \
  --partition e2e-test \
  --file-id test-doc-1 \
  --name "e2e-test.txt" \
  --rag-base-url "http://localhost:8083" \
  --rag-api-key "" \
  upsert-file /tmp/e2e-test.txt
```

**Step 3: Start the consumer (if not already running)**

```bash
uv run python -m rag_indexer.main
```

Watch logs for `message_received` → `message_processed` with `status=success`.

**Step 4: Verify file in OpenRAG**

```bash
curl -s http://localhost:8083/partition/e2e-test/file/test-doc-1 | python -m json.tool
```

Expected: 200 with file metadata and chunks.

---

### Task 9: E2E — Idempotent re-index (same version)

**Step 1: Publish the same message again**

```bash
uv run python scripts/producer.py \
  --partition e2e-test \
  --file-id test-doc-1 \
  --name "e2e-test.txt" \
  --rag-base-url "http://localhost:8083" \
  --rag-api-key "" \
  upsert-file /tmp/e2e-test.txt
```

**Step 2: Observe consumer logs**

Expected: Consumer receives message, does GET, finds matching version/md5sum, skips re-indexing. Log should show `message_processed` with `status=success` but no upsert call.

---

### Task 10: E2E — Update existing file

**Step 1: Create modified file**

```bash
echo "Updated content — this is version 2 of the test document." > /tmp/e2e-test-v2.txt
```

**Step 2: Publish upsert with same file_id but new content**

```bash
uv run python scripts/producer.py \
  --partition e2e-test \
  --file-id test-doc-1 \
  --name "e2e-test.txt" \
  --rag-base-url "http://localhost:8083" \
  --rag-api-key "" \
  upsert-file /tmp/e2e-test-v2.txt
```

**Step 3: Observe consumer logs**

Expected: GET returns 200 with old version → md5 differs → PUT update → success.

**Step 4: Verify updated in OpenRAG**

```bash
curl -s http://localhost:8083/partition/e2e-test/file/test-doc-1 | python -m json.tool
```

---

### Task 11: E2E — Delete file

**Step 1: Publish delete message**

```bash
uv run python scripts/producer.py \
  --partition e2e-test \
  --file-id test-doc-1 \
  --rag-base-url "http://localhost:8083" \
  --rag-api-key "" \
  delete
```

**Step 2: Observe consumer logs**

Expected: DELETE returns 204 → success.

**Step 3: Verify file removed**

```bash
curl -s http://localhost:8083/partition/e2e-test/file/test-doc-1
```

Expected: 404.

---

### Task 12: E2E — Delete non-existent file (idempotent)

**Step 1: Publish delete for non-existent file**

```bash
uv run python scripts/producer.py \
  --partition e2e-test \
  --file-id does-not-exist \
  --rag-base-url "http://localhost:8083" \
  --rag-api-key "" \
  delete
```

**Step 2: Observe consumer logs**

Expected: DELETE returns 404 → treated as success (idempotent).

---

### Task 13: E2E — Transient error and retry

**Step 1: Publish message with unreachable RAG URL**

```bash
uv run python scripts/producer.py \
  --partition e2e-test \
  --file-id retry-test \
  --rag-base-url "http://localhost:9999" \
  --rag-api-key "" \
  upsert-file /tmp/e2e-test.txt
```

**Step 2: Observe consumer logs**

Expected: Network error → TransientError → retry queue progression → eventually DLQ after 3 retries.

**Step 3: Verify message in DLQ**

```bash
uv run python scripts/dlq_replay.py list
```

Expected: Shows the retry-test message.

---

### Task 14: Fix any bugs discovered during E2E testing

If E2E tests reveal additional bugs (they likely will), fix them here. Common things to watch for:

- OpenRAG response format not matching what `processing.py` expects from `rag_get_file`
- Partition creation requirements
- Authentication issues
- Content-type mismatches in multipart upload
- Missing `async with` or response reading issues

For each bug: diagnose from logs → write fix → run unit tests → re-run the failing E2E scenario → commit.
