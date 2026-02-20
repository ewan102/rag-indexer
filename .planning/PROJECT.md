# RAG Indexer

## What This Is

A production-grade RabbitMQ consumer service that indexes files into a RAG (Retrieval-Augmented Generation) system. It listens for indexing messages on a topic exchange, fetches or receives file content, and pushes it to a RAG HTTP API. It handles retries with configurable exponential backoff, routes permanently failed messages to a dead letter queue with CLI replay tooling, processes messages concurrently, and exposes structured logging with Prometheus metrics. Runs as a Docker container in production at Linagora.

## Core Value

No message is ever silently lost — every indexing request either succeeds, retries with backoff, or lands in a dead letter queue where it can be inspected and replayed.

## Requirements

### Validated

- ✓ Consume messages from RabbitMQ topic exchange — existing
- ✓ Parse message headers into validated Pydantic models — existing
- ✓ Check RAG for existing file version before indexing — existing
- ✓ Upsert files to RAG via multipart HTTP (POST new, PUT existing) — existing
- ✓ Delete files from RAG via HTTP DELETE — existing
- ✓ Skip indexing when file version matches (md5sum) — existing
- ✓ Support dual content modes (direct binary, URL download) — existing
- ✓ Retry transient failures (5xx) with exponential backoff (30s, 5m, 1h) — existing
- ✓ Route fatal failures (4xx, validation errors) to DLQ — existing
- ✓ Producer CLI script for testing message publishing — existing
- ✓ Fix critical bugs (response outside context manager, broken metadata merge) — v1.0
- ✓ Add proper error hierarchy instead of RuntimeError/Exception classification — v1.0
- ✓ Ensure queue and message durability (persistent delivery mode, durable queues) — v1.0
- ✓ Graceful shutdown — handle SIGTERM, finish in-flight message before stopping — v1.0
- ✓ Dockerfile for containerized deployment — v1.0
- ✓ Restructure from single-file POC to maintainable module layout — v1.0
- ✓ Comprehensive test suite covering retry flow, edge cases, and integration paths — v1.0
- ✓ Structured JSON logging with per-message context correlation (structlog) — v1.0
- ✓ Prometheus metrics endpoint (/metrics) with message counters and duration histogram — v1.0
- ✓ JSON health endpoint (/health) with RabbitMQ connection check — v1.0
- ✓ Sensitive field scrubbing (API keys, bearer tokens never appear in logs) — v1.0
- ✓ DLQ replay capability — inspect and re-queue failed messages — v1.0
- ✓ Configurable retry intervals via environment variable — v1.0
- ✓ Concurrent message processing — process multiple messages in parallel — v1.0

### Active

(None — v1.0 shipped, next milestone not yet planned)

### Out of Scope

- Web UI for DLQ management — CLI replay is sufficient for v1
- Multi-broker RabbitMQ clustering — single broker assumed
- File streaming for very large files (>1GB) — can revisit if needed
- Custom RAG API client SDK — aiohttp direct calls are fine

## Context

- Shipped v1.0 with 2,302 LOC Python across 29 files
- Tech stack: Python 3.12+, aio-pika, aiohttp, pydantic v2, structlog, prometheus-client
- The RAG service is Linagora's "Ragondin" platform (HTTP REST API with bearer auth)
- RabbitMQ credentials and RAG API keys are passed per-message in headers
- The consumer is stateless — all state travels with the message or lives in RabbitMQ/RAG
- Package: `rag_indexer` with 8 modules (config, models, errors, rag_client, processing, transport, logging, metrics)
- Test suite: 45 tests (39 unit + 6 integration against real RabbitMQ via Docker Compose)
- Observability: structlog JSON logging, Prometheus /metrics, JSON /health
- Operational: DLQ replay CLI (scripts/dlq_replay.py), configurable retry intervals and concurrency
- Known tech debt: `rag_base_url` from message headers not validated against allowlist (SSRF vector)

## Constraints

- **Tech stack**: Python 3.12+, aio-pika, aiohttp, pydantic v2, structlog, prometheus-client — established
- **Package manager**: uv — already in use with lockfile
- **Deployment**: Docker container
- **Message format**: RabbitMQ headers-based message contract — must remain compatible with existing producers
- **RAG API**: External HTTP service — consumer adapts to its API, not the other way around

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Keep Python + aio-pika stack | POC already works, team knows it, async I/O fits the workload | Confirmed |
| Headers-based message contract | Already established, producers depend on it | Confirmed |
| Custom error hierarchy (TransientError/FatalError) | RuntimeError/Exception classification is fragile and implicit | Shipped v1.0 |
| Flat module structure (no __init__.py re-exports) | Explicit imports, no circular dependency risk | Shipped v1.0 |
| Quorum queues for main + DLQ | Replicated, durable by default, raft consensus | Shipped v1.0 |
| x-death entry counting for retries | x-delivery-count incompatible with escalating delays | Shipped v1.0 |
| structlog with contextvars | Async-safe per-message context binding, automatic log correlation | Shipped v1.0 |
| prometheus-client with aiohttp handler | Official Python client, native aiohttp support, single /metrics route | Shipped v1.0 |
| Quiet verbosity (INFO: receive + outcome only) | Minimize log noise in normal operation, DEBUG for detail | Shipped v1.0 |
| pytest-docker for integration tests | Real broker validation, auto-skip when Docker unavailable | Shipped v1.0 |
| TTL-embedded queue names | Avoids PRECONDITION_FAILED on retry queue redeclaration | Shipped v1.0 |
| queue.consume() + Semaphore for concurrency | Callback-based for explicit task tracking, semaphore for bounding | Shipped v1.0 |
| 10s drain timeout on shutdown | Matches Docker's default stop grace period | Shipped v1.0 |

---
*Last updated: 2026-02-20 after v1.0 milestone*
