# Automated E2E Tests

**Date:** 2026-02-24
**Status:** Approved

## Context

The manual E2E shakedown found real bugs but left no automated tests. This design adds a rerunnable E2E test suite that exercises the full pipeline: publish message to RabbitMQ, consumer processes it, RAG API is called, state is verified.

## Architecture

One test file `tests/e2e/test_e2e.py` with 6 test functions. A `rag_base_url` fixture provides either a stateful in-process stub server (default) or `localhost:8083` (with `--e2e-live` flag).

Tests run the full pipeline: publish via RabbitMQ exchange → consume from queue → `process_message()` → verify RAG state.

## Stateful RAG Stub

An aiohttp TestServer backed by `dict[tuple[str, str], dict]` that mimics OpenRAG:

- `GET /partition/{p}/file/{fid}` → 200 with metadata if exists, 404 if not
- `POST /indexer/partition/{p}/file/{fid}` → store file, return 201; or 409 if exists
- `PUT /indexer/partition/{p}/file/{fid}` → update stored file, return 202
- `DELETE /indexer/partition/{p}/file/{fid}` → remove, return 204; or 404 if not found

The stub parses the multipart `metadata` field to extract and store the `version` value, enabling the consumer's version-check logic to work realistically.

## Consumer Integration

Tests publish messages to the RabbitMQ exchange, then consume one message from the queue and call `process_message()` directly with a real aiohttp session. This tests the full code path including header parsing, version checking, and HTTP calls.

## Test Scenarios

1. **Upsert new file** → file not in RAG → POST → verify metadata stored
2. **Idempotent skip** → same file+md5 → GET shows matching version → no PUT
3. **Update existing** → same file_id, different md5 → PUT → verify updated
4. **Delete file** → DELETE → verify 404 after
5. **Delete non-existent** → DELETE on missing file → 404 treated as success
6. **Transient error** → unreachable RAG URL → TransientError raised

## --e2e-live Flag

- `pytest_addoption` in `tests/e2e/conftest.py`
- Default (no flag): stateful stub, self-contained, CI-friendly
- `--e2e-live`: real OpenRAG at localhost:8083, creates/cleans `e2e-test` partition

## Files

- `tests/e2e/__init__.py`
- `tests/e2e/conftest.py` — fixtures: rag_stub, rag_base_url, rmq channel, publish/consume helpers
- `tests/e2e/test_e2e.py` — 6 test functions
