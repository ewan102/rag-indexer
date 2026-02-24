# rag-indexer

Async RabbitMQ consumer that indexes documents into a RAG API. Listens for file upsert/delete messages, forwards them to a configurable RAG backend, and handles retries with exponential back-off via TTL delay queues.

## Architecture

```
Producer ──► RabbitMQ (topic exchange) ──► rag-indexer ──► RAG API
                  ▲                              │
                  │  TTL delay queues             │ on transient error
                  └───────── retry ◄──────────────┘
                                                  │ on fatal / exhausted
                                                  └──► DLQ
```

- **Main queue** (`rag.index.q`) — quorum queue bound to `rag.index.*`
- **Retry queues** — classic TTL queues (default: 30s, 5m, 1h) that dead-letter back to the main exchange
- **DLQ** (`rag.index.dlq`) — quorum queue for poison messages

## Quick start

```bash
# clone & install
uv sync

# copy env and fill in RAG credentials
cp env.example .env

# run the consumer
uv run python -m rag_indexer
```

## Configuration

All settings are via environment variables (see `env.example`):

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | AMQP connection string |
| `RAG_BASE_URL` | — | RAG API base URL |
| `RAG_API_KEY` | — | RAG API bearer token |
| `CONCURRENCY` | `1` | Max messages processed in parallel |
| `HTTP_TIMEOUT` | `60` | HTTP request timeout (seconds) |
| `RETRY_INTERVALS` | `30000,300000,3600000` | Comma-separated retry delays (ms) |
| `LOG_LEVEL` | `INFO` | Logging level |

## Docker

```bash
docker build -t rag-indexer .
docker run --env-file .env rag-indexer
```

Exposes `:8080/health` (RabbitMQ liveness) and `:8080/metrics` (Prometheus).

## Scripts

- `scripts/producer.py` — CLI to publish upsert/delete messages to the exchange
- `scripts/dlq_replay.py` — List and replay messages stuck in the DLQ

## Tests

```bash
make test        # unit tests only
make test-all    # unit + integration (requires Docker for RabbitMQ)
```
