# E2E Shakedown: Bug Fixes + Live Testing

**Date:** 2026-02-24
**Status:** Approved

## Context

The rag-indexer v1.0 was built and unit-tested but never run end-to-end against a real OpenRAG instance. This design covers fixing bugs found during code review against the OpenRAG API, then validating the full pipeline with live services.

## Phase A: Bug Fixes

### Fix 1: Add dotenv loading to consumer

`config.py` uses `os.getenv()` but never calls `load_dotenv()`. The `.env` file is ignored unless variables are exported in the shell. Add `python-dotenv` loading at the top of `config.py`.

### Fix 2: aiohttp timeout type mismatch

`rag_client.py` passes `timeout=HTTP_TIMEOUT` (an `int`) to aiohttp request methods. aiohttp expects `aiohttp.ClientTimeout`. Convert `HTTP_TIMEOUT` to a proper `ClientTimeout` object in `config.py`.

### Fix 3: Connection leaks in rag_get_file and rag_delete

Both functions return a raw `aiohttp.ClientResponse` without using `async with`. The response body is read by the caller but the connection is never properly released. Refactor to return structured data (status code + parsed body) instead of raw response objects.

### Fix 4: Handle 409 Conflict on POST

OpenRAG returns `409 Conflict` when POSTing a file that already exists. The GET-then-POST logic should prevent this, but race conditions can trigger it. Instead of sending 409 to DLQ as a FatalError, treat it as a retriable condition (retry with PUT).

### Fix 5: Fix rag_upsert return type

Function declares `-> aiohttp.ClientResponse` but returns `None` after reading the response inside a context manager. Fix the type annotation to `-> None`.

## Phase B: E2E Testing

### Infrastructure

- **RabbitMQ:** docker-compose with `rabbitmq:3-management` on default ports (5672 + 15672)
- **OpenRAG:** Already running on `localhost:8083` (no auth token configured)

### Test Scenarios

1. **Happy path upsert** - Send a `.txt` file via producer, verify consumer processes it, verify file exists in OpenRAG via GET
2. **Idempotent re-index** - Send same file with same md5sum, verify consumer skips re-indexing
3. **Update existing file** - Send same file_id with different content, verify PUT update succeeds
4. **Delete file** - Send delete action, verify file removed from OpenRAG
5. **Delete non-existent** - Send delete for non-existent file_id, verify idempotent success
6. **Transient error / DLQ** - Trigger a transient failure, verify retry queue progression

### Execution

Use the existing `scripts/producer.py` CLI to publish messages. Run the consumer with `uv run python -m rag_indexer.main`. Observe structured logs and verify state via OpenRAG API and RabbitMQ management UI.

## Out of Scope

- Async task status polling (OpenRAG queues indexing internally; consumer's job is to submit)
- SSRF allowlist for rag_base_url
- File streaming for large files
- Automated E2E test suite (this is a manual shakedown)
