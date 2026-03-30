# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                          # install/update dependencies
uv run python -m rag_indexer     # run the consumer
make test                        # unit tests only
make test-all                    # unit + integration (needs Docker for RabbitMQ)
make lint                        # ruff linter
uv run pytest tests/unit/test_processing.py -v   # run a single test file
uv run pytest tests/unit/ -v -k "test_name"      # run a single test by name
```

Integration tests use `pytest-docker` to spin up RabbitMQ automatically. E2E tests live in `tests/e2e/` and use a stateful RAG stub.

## Architecture

Async RabbitMQ consumer that indexes documents into a RAG API with retry and dead-letter handling.

```
Producer -> RabbitMQ (topic exchange) -> rag-indexer -> RAG API
                ^                            |
                | TTL delay queues           | on transient error
                +-------- retry <------------+
                                             | on fatal / exhausted
                                             +-> DLQ
```

**Message flow through the code:**

1. `main.py` — Entry point. Connects to RabbitMQ, starts health server, consumes messages with concurrency control via `asyncio.Semaphore`. Routes errors to retry queues or DLQ.
2. `processing.py` — Core business logic. Parses message headers into `IndexMessage`, handles upsert (GET-then-PUT with version check) and delete paths. Classifies errors as `TransientError` (retryable) or `FatalError` (goes to DLQ).
3. `rag_client.py` — HTTP client for the RAG API (GET file, DELETE, UPSERT with multipart form).
4. `transport.py` — RabbitMQ topology declaration (main queue, retry queues, DLQ), publish helpers, health/metrics HTTP server, shutdown coordination.
5. `config.py` — All configuration from env vars. Retry queue names are generated from TTL intervals (e.g., `rag.index.retry.30s.q`).
6. `models.py` — Pydantic models: `IndexMessage`, `RagConn`, `ContentSpec`.

**Key design decisions:**

- Messages carry metadata in AMQP headers and file binary in the body (not JSON-encoded).
- RAG connection info (`rag_base_url`, `rag_api_key`) comes per-message via headers, not from global config.
- Retry count is derived from `x-death` header entries with `reason=expired`, not a custom counter.
- Upsert is idempotent: skips indexing if remote version matches local `version`/`md5sum`.
- Quorum queues for main queue and DLQ; classic queues for TTL retry delays.

## Error Classification

- `TransientError` — HTTP 429, 5xx, timeouts, network errors -> retry queue
- `FatalError` — HTTP 4xx (except 429), unknown actions, unexpected errors -> DLQ
- `ValidationError` (Pydantic) -> DLQ directly
