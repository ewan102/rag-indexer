# Coding Conventions

**Analysis Date:** 2026-02-17

## Naming Patterns

**Files:**
- Lowercase with underscores: `consumer.py`, `producer.py`
- Script files in `scripts/` directory use same convention
- Test files use `test_<module>.py` pattern: `test_consumer.py`

**Functions:**
- Lowercase with underscores (snake_case)
- Prefix with context when appropriate: `rag_get_file()`, `rag_delete()`, `rag_upsert()`, `build_metadata()`, `build_headers()`
- Async functions use `async def`: `async def rag_get_file()`, `async def process_message()`
- Command functions prefixed with `cmd_`: `cmd_upsert_file()`, `cmd_upsert_url()`, `cmd_delete()`
- Utility functions like `next_retry_routing_key()`, `md5_of_bytes()`, `guess_content_type()`

**Variables:**
- Lowercase with underscores (snake_case)
- Constants in UPPER_CASE: `RABBITMQ_URL`, `EXCHANGE_NAME`, `MAX_RETRIES`, `HTTP_TIMEOUT`
- Private helpers prefixed with underscore: `_S` (test fixture), `_json`, `_text`
- Loop variables and temporary: `e`, `resp`, `msg`, `data`, `file`, `meta`

**Types:**
- Classes use PascalCase: `RagConn`, `ContentSpec`, `IndexMessage`, `FakeResp`, `DummyMessage`
- Type hints use standard Python typing: `Optional[str]`, `Dict[str, Any]`, `bytes | None` (Python 3.10+)
- BaseModel used for data validation: Pydantic models for message schemas

## Code Style

**Formatting:**
- No explicit formatter detected (no `.prettierrc`, `.ruff.toml`, or `black` config)
- Indentation: 4 spaces (standard Python)
- Line length: appears to follow reasonable limits (~80-100 chars, no hard rule enforced)
- Comments: Mixed English/French (see code at lines 149, 164, 244, 320-321)

**Linting:**
- No linting configuration file detected (no `.pylintrc`, `.flake8`, or `ruff.toml`)
- Pytest configuration in `pyproject.toml`:
  ```toml
  [tool.pytest.ini_options]
  asyncio_mode = "auto"
  addopts = "-q"
  ```
- Development dependencies include `pytest>=8.0` and `pytest-asyncio>=0.23`

## Import Organization

**Order (observed pattern in both `consumer.py` and `scripts/producer.py`):**
1. Standard library imports (asyncio, json, os, sys, types, argparse, hashlib, mimetypes)
2. Third-party library imports (aiohttp, aio_pika, aiormq, pydantic, dotenv)
3. Local module imports (only in tests: `import consumer`)

**Pattern from `consumer.py`:**
```python
import asyncio
import json
import os
import sys
from typing import Optional, Dict, Any

import aiohttp
import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aiormq import AMQPConnectionError
from pydantic import BaseModel, Field, ValidationError
from aiohttp import FormData
```

**Pattern from `scripts/producer.py`:**
```python
#!/usr/bin/env python3
import asyncio
import os
import sys
import argparse
import hashlib
import mimetypes
from typing import Optional

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aiormq import AMQPConnectionError
from dotenv import load_dotenv
```

**Path Aliases:**
- No path aliases or `__init__.py` imports detected
- Direct module references used

## Error Handling

**Patterns:**
- **Exception types differentiated by purpose:**
  - `RuntimeError`: Transient/retry-able errors (5xx, timeouts, pannes RAG)
  - `ValidationError` (Pydantic): Invalid payload schema → DLQ direct, no retry
  - `Exception`: Non-retry fatal errors (4xx config/data issues)
  - General `Exception`: Unknown action/fallback

- **Error classification in main loop** (`consumer.py` lines 360-380):
  ```python
  except ValidationError as ve:
      # Mauvais payload -> DLQ direct (non-retry)
      await republish_to_retry(channel, message, MAX_RETRIES)
  except RuntimeError as transient:
      # Erreurs supposées transitoires (5xx, timeouts, pannes RAG)
      await republish_to_retry(channel, message, retry_count + 1)
  except Exception as fatal:
      # Erreurs 4xx/config/données invalides -> pas de retry prolongé
      await republish_to_retry(channel, message, MAX_RETRIES)
  ```

- **HTTP response status handling:**
  - Status 5xx → RuntimeError (retry-able)
  - Status 4xx → Exception (non-retry, fatal)
  - Status 200-299 → Success
  - Status 404 → Context-dependent (404 on GET delete is OK, 404 on GET upsert means new file)

- **Error messages include context:**
  - `f"RAG {method} 5xx: {resp.status} {resp_text}"`
  - `f"RAG {method} failed {resp.status}: {resp_text}"`
  - `f"Failed to fetch file_url ({resp.status})"`

## Logging

**Framework:** `print()` with standard Python output (not a dedicated logging library)

**Patterns:**
- Console output via `print()` in main flow
- Standard output for info: `print("process new message", msg)`
- Standard error for errors: `print(..., file=sys.stderr)` (see line 362, 370, 376)
- Format: `print(f"[TAG] message")` for categorized output:
  - `[NORETRY]` for invalid payload
  - `[RETRY]` for transient errors
  - `[NORETRY]` for fatal errors

**Example from lines 362, 370, 376:**
```python
print(f"[NORETRY] invalid payload: {ve}", file=sys.stderr)
print(f"[RETRY] transient error: {transient}", file=sys.stderr)
print(f"[NORETRY] fatal error: {fatal}", file=sys.stderr)
```

## Comments

**When to Comment:**
- Explain algorithm choices and state transitions (see lines 149-150, 164, 244, 320-321)
- Mark section boundaries with `# ---------- [Section] ----------` (pervasive pattern)
- Document non-obvious logic like retry/DLQ routing and GET/POST vs GET/PUT decisions

**JSDoc/TSDoc:**
- Not detected in codebase (Python-specific, no docstrings observed)
- Type hints used instead (see function signatures with `-> Optional[str]`, `-> Dict[str, Any]`)

**Section comments (observed pattern):**
```python
# ---------- Config via env ----------
# ---------- Message schema ----------
# ---------- Helpers ----------
# ---------- HTTP calls to RAG ----------
# ---------- Processing ----------
# ---------- Retry / DLQ publishing ----------
# ---------- Topology declaration ----------
# ---------- Consumer loop ----------
```

## Function Design

**Size:**
- Functions range from 3-20 lines for utilities to 40+ lines for complex workflows
- Main async handler `process_message()`: 80+ lines (lines 175-258) - handles delete and upsert flows
- Main event loop `main()`: 50+ lines (lines 332-380) - connection, topology, message iteration

**Parameters:**
- Async functions receive positional parameters: `async def rag_get_file(session, rag, partition, file_id)`
- Complex parameters passed as objects: `msg: IndexMessage` instead of individual fields
- Keyword-only arguments used in builder functions: `def build_headers(*, action, partition, ...)` (line 45)
- Optional parameters have defaults: `Optional[str] = None`

**Return Values:**
- HTTP operations return response objects: `-> aiohttp.ClientResponse`
- Metadata builders return dicts: `-> Dict[str, Any]`
- Simple helpers return primitives: `-> Optional[str]`, `-> str`
- Async void operations: most message processing functions return `None` implicitly

## Module Design

**Exports:**
- Top-level: Config constants loaded from environment
- Functions are module-level and directly importable (test imports `consumer` and calls `consumer.process_message()`)
- Pydantic models (RagConn, ContentSpec, IndexMessage) exposed at module level

**Pattern from `consumer.py` module structure:**
```python
# 1. Imports
# 2. Environment configuration
# 3. Message schema classes
# 4. Helper functions
# 5. HTTP wrapper functions
# 6. Main processing function
# 7. Retry/DLQ publishing
# 8. Topology declaration
# 9. Consumer loop entry point
```

**Pattern from `scripts/producer.py` module structure:**
```python
# 1. Imports
# 2. Environment configuration
# 3. Utility functions (md5, mime type, header building)
# 4. Publishing function
# 5. Subcommand handlers (cmd_upsert_file, cmd_upsert_url, cmd_delete)
# 6. Argument parser builder
# 7. Async main entry point
```

**No barrel files** - Direct imports preferred, minimal re-exports

---

*Convention analysis: 2026-02-17*
