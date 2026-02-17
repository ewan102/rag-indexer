# Technology Stack

**Analysis Date:** 2026-02-17

## Languages

**Primary:**
- Python 3.12 - Entire application codebase

**Runtime Configuration:**
- `.python-version`: Specifies Python 3.12 requirement

## Runtime

**Environment:**
- Python 3.12+ (as specified in `pyproject.toml`: `requires-python = ">=3.12"`)

**Package Manager:**
- `uv` (UV package manager)
- Lockfile: `uv.lock` present

## Frameworks

**Core Async Framework:**
- `aio-pika` 9.4+ - AMQP message queue consumer/producer
  - Handles RabbitMQ connection and messaging topology
  - Location: `consumer.py`, `scripts/producer.py`

**HTTP Client:**
- `aiohttp` 3.10+ - Async HTTP client for RAG API requests
  - Used for GET/POST/PUT/DELETE operations to RAG service
  - Location: `consumer.py` (functions like `rag_get_file()`, `rag_upsert()`, `rag_delete()`)

**Data Validation:**
- `pydantic` 2.7+ - Dataclass-style validation models
  - Location: `consumer.py` defines `RagConn`, `ContentSpec`, `IndexMessage` models

**Environment Configuration:**
- `python-dotenv` 1.2.1+ - .env file loading for local development
  - Location: Used in `scripts/producer.py` for local testing

**Testing:**
- `pytest` 8.0+ - Test runner
  - Config: `pyproject.toml` with `asyncio_mode = "auto"`
- `pytest-asyncio` 0.23+ - Async test support
  - Enables `@pytest.mark.asyncio` decorator in `tests/test_consumer.py`

## Key Dependencies

**Critical:**
- `aio-pika` 9.4+ - Why it matters: Core message queue consumer; application cannot function without RabbitMQ connectivity
- `aiohttp` 3.10+ - Why it matters: All file indexing and deletion operations depend on RAG HTTP API calls
- `pydantic` 2.7+ - Why it matters: Message payload validation prevents invalid data from being processed

**Infrastructure:**
- `aiormq` - Implicit dependency of `aio-pika`; provides low-level AMQP protocol handling
- `asyncio` - Python stdlib; enables async/await throughout the application

## Configuration

**Environment:**
- Configuration loaded from environment variables with defaults
- `.env` file present for local development (never committed to git)
- `env.example` provides template with example values
  - Contains: `RABBITMQ_URL`, `RAG_BASE_URL`, `RAG_API_KEY`

**Key Environment Variables:**
- `RABBITMQ_URL` - RabbitMQ connection string (default: `amqp://guest:guest@localhost:5672/`)
- `EXCHANGE_NAME` - Topic exchange name (default: `rag.index.topic`)
- `ROUTING_KEY` - Main queue routing key (default: `rag.index.*`)
- `QUEUE_NAME` - Main queue name (default: `rag.index.q`)
- `RETRY_EXCHANGE` - Retry exchange name (default: `rag.index.retry.x`)
- `MAX_RETRIES` - Maximum retry attempts before DLQ (default: 3)
- `DLQ_NAME` - Dead letter queue name (default: `rag.index.dlq`)
- `HTTP_TIMEOUT` - HTTP request timeout in seconds (default: 60)

**Consumer Configuration** (in `consumer.py`):
- Lines 15-30: All configuration loaded from environment at module initialization
- Retry queue tiers: 30 seconds, 5 minutes, 1 hour (line 21-25)

**Producer Configuration** (in `scripts/producer.py`):
- Lines 18-25: Same environment variables
- Can be overridden via CLI arguments

## Build & Development

**Project Metadata:**
- `pyproject.toml` - Single source of truth for dependencies and configuration
- `uv.lock` - Dependency lockfile (reproducible builds)

**Project Structure:**
- `consumer.py` - Main async consumer application
- `scripts/producer.py` - Message producer CLI tool for testing
- `tests/test_consumer.py` - Test suite for consumer logic

## Platform Requirements

**Development:**
- Python 3.12+
- RabbitMQ server (AMQP 0.9.1 compatible)
- RAG indexer HTTP API service running

**Production:**
- Python 3.12+
- RabbitMQ broker (persistent queues enabled)
- Network connectivity to RAG API service

---

*Stack analysis: 2026-02-17*
