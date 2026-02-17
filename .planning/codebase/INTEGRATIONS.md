# External Integrations

**Analysis Date:** 2026-02-17

## APIs & External Services

**RAG Indexer Service:**
- Purpose: Central document indexing and retrieval service
  - SDK/Client: `aiohttp.ClientSession` (async HTTP)
  - Auth: Bearer token in `Authorization` header
  - Env var: `RAG_BASE_URL` (e.g., `https://ragondin-twake-staging.linagora.com`)
  - API Key: `RAG_API_KEY` (bearer token)
  - Base URL: Configured via `RAG_BASE_URL` environment variable

**RAG Service Endpoints:**
- GET `/partition/{partition}/file/{file_id}` - Check if file exists and get metadata
  - Used in: `rag_get_file()` function, line 70-76 in `consumer.py`
  - Purpose: Determine if file needs indexing (version comparison)

- POST `/indexer/partition/{partition}/file/{file_id}` - Create new indexed file
  - Used in: `rag_upsert()` function, line 113-172 in `consumer.py`
  - Purpose: Upload and index new documents
  - Content: Multipart form with file binary + metadata JSON
  - Query params: `parent_id`, `name`, `md5sum` (optional)

- PUT `/indexer/partition/{partition}/file/{file_id}` - Update existing indexed file
  - Used in: `rag_upsert()` function (method selection based on `is_new` flag)
  - Purpose: Update content of existing indexed file
  - Same multipart format as POST

- DELETE `/indexer/partition/{partition}/file/{file_id}` - Remove indexed file
  - Used in: `rag_delete()` function, line 79-85 in `consumer.py`
  - Purpose: Delete file from index
  - Idempotent: Returns 2xx or 404 as success

**Error Handling per Status:**
- 2xx - Success
- 4xx - Configuration/data error; no retry (non-transient)
- 5xx - Service error; trigger retry mechanism

## Message Queue

**RabbitMQ:**
- Connection: `aio-pika` async client
- URL: `RABBITMQ_URL` env var (default: `amqp://guest:guest@localhost:5672/`)
- Protocol: AMQP 0.9.1

**Message Flow:**

1. **Main Exchange:** Topic exchange `rag.index.topic` (durable)
2. **Main Queue:** `rag.index.q` bound with routing key `rag.index.*`
3. **Retry Mechanism:**
   - Three retry queues with TTL: 30s, 5m, 1h
   - Retry exchange: `rag.index.retry.x` (direct, durable)
   - Failed messages: Republished with incremented `x-retry-count` header
4. **Dead Letter Queue:** `rag.index.dlq` (for exhausted retries)

**Queue Declaration:** `declare_topology()` function, line 293-328 in `consumer.py`

## Authentication & Identity

**Auth Provider:**
- Custom bearer token authentication
  - Implementation: Bearer token passed in HTTP Authorization header to RAG API
  - Token source: Environment variable `RAG_API_KEY`
  - Per-message override: Headers can specify `rag_api_key` in message metadata

**Message Headers Authentication:**
- `rag_base_url` - RAG service endpoint (per-message, can override env default)
- `rag_api_key` - RAG service bearer token (per-message, can override env default)
- `file_bearer` - Optional bearer token for downloading files from `file_url`

## External File Retrieval

**File Download via URL:**
- Used when producer specifies `file_url` instead of sending binary in message body
- Function: `get_producer_file()`, line 88-98 in `consumer.py`
- Auth: Optional bearer token via `file_bearer` header
- Timeout: Uses `HTTP_TIMEOUT` config (default 60 seconds)
- Retry: Failures trigger message retry mechanism

## Message Schema

**Incoming Message Structure** (from RabbitMQ headers):
```
Headers:
- action: "upsert" | "delete"
- partition: string (tenant/user identifier)
- file_id: string (unique file identifier)
- rag_base_url: string (RAG API endpoint)
- rag_api_key: string (RAG API bearer token)
- doctype: string (optional, document type classification)
- version: string (optional, version identifier)
- md5sum: string (optional, content hash)
- name: string (optional, display filename)
- dir_id: string (optional, parent directory reference)
- datetime: string (optional, ISO datetime)
- content_type: string (optional, MIME type)
- file_url: string (optional, URL to download file from)
- file_bearer: string (optional, bearer token for file_url download)
- x-retry-count: int (internal, tracks retry attempts)

Body:
- Binary file content (for upsert) or empty (for delete/url-based upsert)
```

**Pydantic Models** (validation in `consumer.py`):
- `RagConn` (line 33-35): RAG service credentials
- `ContentSpec` (line 38-42): Content source specification
- `IndexMessage` (line 44-58): Complete message schema

## Error Handling & Retry Logic

**Retry Classification:**
1. **Transient Errors** (trigger retry):
   - 5xx HTTP responses from RAG API
   - Network timeouts
   - Raised as `RuntimeError`
   - Republished to next retry queue

2. **Non-Retry Errors** (go to DLQ):
   - Invalid message payload (`ValidationError`)
   - 4xx HTTP responses (config/auth issues)
   - Raised as generic `Exception`
   - Republished immediately to DLQ

3. **Retry Schedule:**
   - Attempt 0: Immediate processing
   - Attempt 1: After 30 seconds
   - Attempt 2: After 5 minutes
   - Attempt 3: After 1 hour
   - Attempt 4+: Dead letter queue (no further retries)

**Configuration:**
- `RETRY_QUEUES` list (line 21-25): Queue names and TTL values
- `MAX_RETRIES` (line 26): Default 3 (matches queue count)

## Webhooks & Callbacks

**Incoming:**
- RabbitMQ message consumption (push model)
  - Queue: `rag.index.q`
  - Consumer: Async iterator via `queue.iterator()` (line 348 in `consumer.py`)

**Outgoing:**
- No webhook callbacks
- All communication with RAG service is request-response over HTTP
- No external event notifications or callbacks

## CI/CD & Deployment

**Testing:**
- Test framework: pytest with pytest-asyncio
- Test file: `tests/test_consumer.py`
- Configuration: Lines 16-18 in `pyproject.toml`
  - `asyncio_mode = "auto"` enables async test detection
  - `addopts = "-q"` quiet output

**Hosting:**
- Not detected; assumes Docker or manual Python environment
- Entry point: `consumer.py` main() function

## Environment Configuration

**Required env vars:**
- `RABBITMQ_URL` - Connection string to RabbitMQ broker
- `RAG_BASE_URL` - Base URL of RAG indexer API
- `RAG_API_KEY` - Bearer token for RAG API authentication

**Optional env vars:**
- `EXCHANGE_NAME` - Override main exchange name
- `ROUTING_KEY` - Override main queue routing key
- `QUEUE_NAME` - Override main queue name
- `RETRY_EXCHANGE` - Override retry exchange name
- `MAX_RETRIES` - Override max retry count
- `DLQ_NAME` - Override dead letter queue name
- `HTTP_TIMEOUT` - Override HTTP request timeout (seconds)

**Secrets location:**
- `.env` file (development only, not committed)
- Environment variables (production deployment)
- Template: `env.example` shows required configuration

## Testing Support

**Producer Script** (`scripts/producer.py`):
- CLI tool for publishing test messages
- Supports three commands:
  - `upsert-file` - Send binary file directly
  - `upsert-url` - Reference external file URL
  - `delete` - Delete indexed file

**Usage Example:**
```bash
python scripts/producer.py \
  --partition user-1 \
  --file-id doc123 \
  upsert-file /path/to/file.pdf
```

**Test Message Factory** (`tests/test_consumer.py`):
- `DummyMessage` class (line 30-35): Simulates RabbitMQ message
- `headers_base` fixture (line 39-46): Common header set
- Monkeypatching: Mocks HTTP calls to isolate consumer logic

---

*Integration audit: 2026-02-17*
