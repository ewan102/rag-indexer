# Architecture

**Analysis Date:** 2026-02-17

## Pattern Overview

**Overall:** Event-driven message queue consumer with async/await patterns for I/O-bound operations.

**Key Characteristics:**
- Async message consumption from RabbitMQ topic exchange
- Exponential backoff retry strategy with dead-letter queue (DLQ)
- HTTP client integration with RAG (Retrieval-Augmented Generation) service
- State-based message processing (GET before PUT/POST)
- Dual content delivery modes (direct binary or URL-based download)

## Layers

**Message Transport Layer:**
- Purpose: Handle RabbitMQ connectivity, topology declaration, queue management
- Location: `consumer.py` lines 293-329 (declare_topology), lines 332-381 (main consumer loop)
- Contains: Exchange declarations, queue binding, retry queue setup, DLQ configuration
- Depends on: aio_pika library, RABBITMQ_URL environment variable
- Used by: Main consumer event loop

**Message Processing Layer:**
- Purpose: Parse and validate incoming messages, orchestrate action routing
- Location: `consumer.py` lines 175-258 (process_message)
- Contains: Message parsing from headers, action dispatching (delete/upsert), error classification
- Depends on: Message Transport Layer, RAG Integration Layer
- Used by: Main consumer loop's exception handling

**RAG Integration Layer:**
- Purpose: Execute HTTP operations against RAG service (retrieve, index, delete)
- Location: `consumer.py` lines 70-171 (rag_get_file, rag_delete, rag_upsert, get_producer_file)
- Contains: HTTP request/response handling, multipart form data construction, metadata building
- Depends on: aiohttp ClientSession, RagConn credentials
- Used by: Message Processing Layer

**Retry & Error Handling Layer:**
- Purpose: Classify failures and route messages to appropriate recovery queues
- Location: `consumer.py` lines 261-289 (republish_to_retry), lines 360-380 (error classification in main loop)
- Contains: Retry count tracking, DLQ promotion logic, exception type mapping
- Depends on: Message Transport Layer, RabbitMQ retry exchange topology
- Used by: Main consumer loop exception handlers

## Data Flow

**Successful UPSERT Flow:**

1. Message consumed from `rag.index.q` topic queue
2. Headers parsed into IndexMessage model (partition, file_id, rag credentials, content metadata)
3. GET request to RAG `/partition/{partition}/file/{file_id}` to check existence and version
   - Response 200: Compare remote md5sum with incoming msg.version
   - Response 404: Mark as new file
   - Response 5xx: Raise RuntimeError (transient error)
   - Response 4xx (other): Raise Exception (config/auth error)
4. If version match detected → message acknowledged, no further action
5. If new or version differs → build multipart form with file content + metadata
6. POST (new=true) or PUT (new=false) to RAG `/indexer/partition/{partition}/file/{file_id}`
7. On success (2xx) → message ACK implicit via context manager
8. On 5xx → RuntimeError raised, triggers retry_count+1
9. On 4xx → Exception raised, jumps to MAX_RETRIES (DLQ)

**Successful DELETE Flow:**

1. Message consumed from `rag.index.q`
2. Headers parsed with action="delete"
3. DELETE request to RAG `/indexer/partition/{partition}/file/{file_id}`
4. If 200 or 404 → message acknowledged (idempotent)
5. If 5xx → RuntimeError, retry_count+1
6. If other 4xx → Exception, MAX_RETRIES→DLQ

**Retry Flow (on transient error):**

1. RuntimeError caught in main loop
2. republish_to_retry called with retry_count+1
3. Message republished to next retry queue (30s, 5m, 1h based on retry_count)
4. Original message ACK'd (consumed from main queue)
5. After TTL expires on retry queue, message dead-letters back to main exchange
6. Message redelivered to main queue via routing key "rag.index.file"

**DLQ Flow (on fatal or exhausted retries):**

1. Exception or max retries reached
2. Message republished with x-retry-count=MAX_RETRIES to DLQ
3. Original message ACK'd
4. DLQ holds message indefinitely for manual inspection/replay

**State Management:**
- Message state encoded in headers: x-retry-count, action, partition, file_id, version
- RAG document state queried via GET before modification (optimistic concurrency)
- No in-memory state kept; all state travels with message or lives in RabbitMQ/RAG

## Key Abstractions

**IndexMessage (Pydantic BaseModel):**
- Purpose: Validated message contract matching headers structure
- Examples: `consumer.py` lines 44-57
- Pattern: Pydantic V2 with Optional fields, nested RagConn and ContentSpec models
- Used to: Ensure type safety and provide IDE autocomplete during processing

**RagConn (Pydantic BaseModel):**
- Purpose: Encapsulate RAG service connection credentials
- Examples: `consumer.py` lines 33-35
- Pattern: Extracted from message headers into dedicated model for clarity
- Used to: Pass credentials cleanly to HTTP operation functions

**ContentSpec (Pydantic BaseModel):**
- Purpose: Abstract dual content delivery modes (note_markdown, file_url, direct binary)
- Examples: `consumer.py` lines 38-41
- Pattern: Optional fields allow different content sources
- Used to: Support flexible indexing (markdown notes, file downloads, binary uploads)

## Entry Points

**Consumer Process:**
- Location: `consumer.py` lines 332-387 (main() and if __name__ block)
- Triggers: Direct script execution or container start
- Responsibilities:
  - Establish RabbitMQ connection with retry
  - Declare topology (exchanges, queues, DLX bindings)
  - Create HTTP client session with timeout
  - Enter async iterator loop consuming messages indefinitely
  - Classify exceptions and trigger appropriate retry/DLQ paths

**Producer Script:**
- Location: `scripts/producer.py` lines 233-243 (amain/main)
- Triggers: CLI invocation with subcommand (upsert-file, upsert-url, delete)
- Responsibilities:
  - Parse CLI arguments for partition, file_id, credentials, content metadata
  - Calculate MD5 for file-based uploads
  - Publish message to exchange with routing key "rag.index.file"
  - Provide 3 subcommand modes for different indexing strategies

## Error Handling

**Strategy:** Exception type-based classification with three recovery paths.

**Patterns:**

- **ValidationError (Pydantic):** Malformed payload/headers → NO_RETRY → DLQ immediately
  - Raised during IndexMessage model validation in process_message
  - Indicates client error (incorrect header structure), not transient
  - Pushed to DLQ to prevent infinite retry loops on garbage data

- **RuntimeError (custom):** Transient infrastructure failures → RETRY
  - Raised when RAG service returns 5xx, timeouts, connection failures
  - Indicates temporary unavailability of downstream service
  - Republished to next retry queue with retry_count+1
  - Examples: "RAG GET 5xx", "RAG POST 5xx", "Failed to fetch file_url"

- **Exception (generic):** Config/auth/data errors → NO_RETRY → DLQ
  - Raised when RAG returns 4xx (except 404), invalid content, unknown actions
  - Indicates problem with message content or credentials, not infrastructure
  - Skipped past retry queues directly to DLQ
  - Examples: "RAG GET failed 401", "No content provided", "Unknown action"

All exceptions caught at line 360-380 in main loop; original message always ACK'd after republishing ensures exactly-once semantics.

## Cross-Cutting Concerns

**Logging:** print() statements to stdout/stderr
- Success messages: "process new message", "go get file" (stdout)
- Errors: "[RETRY]", "[NORETRY]" tags with exception details (stderr)
- No structured logging framework; lines 206, 225, 352, 362, 370, 376

**Validation:** Pydantic models for message schema validation
- IndexMessage, RagConn, ContentSpec require specific fields and types
- Missing required headers (partition, file_id, rag_base_url) trigger ValidationError
- Optional fields allow flexible metadata attachment

**Authentication:** Bearer token in HTTP Authorization headers
- RAG API key passed in message headers, used for all RAG HTTP requests
- File download can use separate file_bearer token (line 90-91)
- No token validation at consumer level; delegated to RAG service

**Concurrency:** Asyncio event loop with aio_pika and aiohttp
- All I/O operations non-blocking (message consumption, HTTP calls, DLX republishing)
- Single consumer instance handles one message at a time (queue.iterator pattern, line 348-350)
- ClientSession reused across all messages (lines 345-346, passed to process_message)
- Channel reused for all publish operations (declared once, used in loop, line 339)

---

*Architecture analysis: 2026-02-17*
