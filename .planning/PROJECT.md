# RAG Indexer

## What This Is

A RabbitMQ consumer service that indexes files into a RAG (Retrieval-Augmented Generation) system. It listens for indexing messages on a topic exchange, fetches or receives file content, and pushes it to a RAG HTTP API. It handles retries with exponential backoff and routes permanently failed messages to a dead letter queue. Runs as a Docker container in production at Linagora.

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

### Active

- [ ] Fix critical bugs (response outside context manager, broken metadata merge)
- [ ] Restructure from single-file POC to maintainable module layout
- [ ] Replace print() with structured logging (levels, timestamps, message IDs)
- [ ] Add proper error hierarchy instead of RuntimeError/Exception classification
- [ ] Ensure queue and message durability (persistent delivery mode, durable queues)
- [ ] Graceful shutdown — handle SIGTERM, finish in-flight message before stopping
- [ ] DLQ replay capability — inspect and re-queue failed messages
- [ ] Metrics/monitoring — expose retry counts, message rates, error rates
- [ ] Concurrent message processing — process multiple messages in parallel
- [ ] Comprehensive test suite covering retry flow, edge cases, and integration paths
- [ ] Dockerfile for containerized deployment

### Out of Scope

- Web UI for DLQ management — CLI replay is sufficient for v1
- Multi-broker RabbitMQ clustering — single broker assumed
- File streaming for very large files (>1GB) — can revisit if needed
- Custom RAG API client SDK — aiohttp direct calls are fine

## Context

- This is a brownfield project — a working POC that needs hardening and extension
- The RAG service is Linagora's "Ragondin" platform (HTTP REST API with bearer auth)
- RabbitMQ credentials and RAG API keys are passed per-message in headers
- The consumer is stateless — all state travels with the message or lives in RabbitMQ/RAG
- Current codebase has critical bugs identified in `.planning/codebase/CONCERNS.md`
- Single `consumer.py` file (~387 lines) handles everything — needs decomposition

## Constraints

- **Tech stack**: Python 3.12+, aio-pika, aiohttp, pydantic v2 — already established
- **Package manager**: uv — already in use with lockfile
- **Deployment**: Docker container
- **Message format**: RabbitMQ headers-based message contract — must remain compatible with existing producers
- **RAG API**: External HTTP service — consumer adapts to its API, not the other way around

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Keep Python + aio-pika stack | POC already works, team knows it, async I/O fits the workload | — Pending |
| Headers-based message contract | Already established, producers depend on it | — Pending |
| Structured logging over print() | Production observability requires levels, timestamps, context | — Pending |
| Custom error hierarchy | RuntimeError/Exception classification is fragile and implicit | — Pending |

---
*Last updated: 2026-02-17 after initialization*
