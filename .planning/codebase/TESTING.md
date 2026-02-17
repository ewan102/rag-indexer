# Testing Patterns

**Analysis Date:** 2026-02-17

## Test Framework

**Runner:**
- pytest 8.0+ (from `pyproject.toml`)
- pytest-asyncio 0.23+ (async test support)
- Configuration in `pyproject.toml`:
  ```toml
  [tool.pytest.ini_options]
  asyncio_mode = "auto"
  addopts = "-q"
  ```

**Assertion Library:**
- pytest built-in assertions (`assert` statements)

**Run Commands:**
```bash
pytest                 # Run all tests
pytest -v              # Verbose mode (despite addopts = "-q")
pytest tests/          # Run tests directory
pytest -k test_delete  # Run specific test
pytest --asyncio-mode=auto  # Explicit async mode
```

## Test File Organization

**Location:**
- Co-located in `tests/` directory at project root
- Pattern: `tests/test_<module>.py` corresponds to `<module>.py`

**Naming:**
- Test file: `tests/test_consumer.py`
- Test functions: `test_<function_name>()` or `test_<scenario>()`
- Async tests: `@pytest.mark.asyncio` decorator required

**Structure:**
```
rag-indexer/
├── consumer.py
├── scripts/
│   └── producer.py
└── tests/
    ├── __init__.py
    └── test_consumer.py
```

## Test Structure

**Suite Organization (from `tests/test_consumer.py`):**
```python
# 1. Imports
import asyncio
import json
import types
import pytest
import consumer

# 2. Helper classes
class FakeResp:
    """Mock HTTP response"""

class DummyMessage:
    """Mock aio_pika message"""

# 3. Fixtures
@pytest.fixture
def headers_base():
    return {...}

@pytest.fixture
def aiohttp_session_stub():
    return _S()

# 4. Test groups by functionality
# - Basic utility tests (test_next_retry_routing_key_ok)
# - DELETE path tests (test_delete_200_ok, test_delete_404_ok, test_delete_5xx_retry_exception)
# - UPSERT GET response handling (test_upsert_get_5xx_raises_runtimeerror, test_upsert_get_4xx_raises_exception)
# - UPSERT state transitions (test_upsert_new_on_404_triggers_post, test_upsert_update_on_md5_change_triggers_put)
# - UPSERT optimization (test_upsert_same_md5_skips)
```

**Patterns:**
- Setup: Fixtures provide base data (`headers_base`, `aiohttp_session_stub`)
- Teardown: No explicit teardown (async context managers handle resource cleanup)
- Assertion: Direct `assert` statements with optional pytest context managers

## Mocking

**Framework:**
- pytest's built-in `monkeypatch` fixture for function/method replacement
- Custom mock classes (`FakeResp`, `DummyMessage`) for response and message mocking
- No external mocking library (unittest.mock, MagicMock) detected

**Patterns:**
```python
# Basic monkeypatch pattern (lines 85-86)
async def fake_delete(session, rag, partition, file_id):
    call_log["called"] = True
    return FakeResp(200, text_data="OK")

monkeypatch.setattr(consumer, "rag_delete", fake_delete)
```

**What to Mock:**
- HTTP response-returning functions: `rag_get_file()`, `rag_delete()`, `rag_upsert()`
- These are replaced with `FakeResp` objects that have `status`, `json()`, `text()`, `read()` methods
- External dependencies (aiohttp sessions) are stubbed, not real instances

**What NOT to Mock:**
- Core message validation and routing logic (let actual code run)
- Pydantic model validation (`IndexMessage`, `RagConn`, etc.)
- Error handling in `process_message()` (verify actual exception types)
- Async context managers (let the test runner handle async lifecycle)

## Fixtures and Factories

**Test Data:**
```python
# Basic fixture pattern (lines 39-46)
@pytest.fixture
def headers_base():
    return {
        "partition": "user-1",
        "file_id": "file-123",
        "rag_base_url": "http://rag:8000",
        "rag_api_key": "secret",
        "content_type": "application/octet-stream",
    }

# Reused in tests via dictionary merge
headers = {**headers_base, "action": "delete"}
headers = {**headers_base, "action": "upsert", "md5sum": "aaa"}
```

**Location:**
- Fixtures defined at top of `tests/test_consumer.py` (lines 38-58)
- Marker comments separate fixture and test sections

**Mock Response Factory:**
```python
class FakeResp:
    def __init__(self, status: int, json_data=None, text_data=""):
        self.status = status
        self._json = json_data
        self._text = text_data

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    async def read(self):
        return b""
```

**Mock Message Factory:**
```python
class DummyMessage:
    """Suffisant pour process_message(): expose .body et .headers."""
    def __init__(self, body: bytes, headers: dict):
        self.body = body
        self.headers = headers
```

## Coverage

**Requirements:**
- No coverage targets or enforcement detected
- `pyproject.toml` has no `[tool.pytest]` coverage configuration
- No `.coveragerc` or coverage config file

**View Coverage:**
```bash
# Coverage not configured, but pytest can be run with coverage.py
pytest --cov=consumer tests/
```

## Test Types

**Unit Tests:**
- Function-level testing via monkeypatching dependencies
- Example: `test_next_retry_routing_key_ok()` (line 64) - tests pure utility function
- Example: `test_delete_200_ok()` - tests `process_message()` with mocked `rag_delete()`

**Integration Tests:**
- Process flow tests that exercise multiple functions
- Example: `test_upsert_new_on_404_triggers_post()` (line 163) - tests GET→404→POST logic
- Example: `test_upsert_update_on_md5_change_triggers_put()` (line 208) - tests GET→200→compare→PUT logic
- These test the decision logic in `process_message()` with mocked HTTP calls

**E2E Tests:**
- Not present in current test suite
- Would require full RabbitMQ and RAG server setup

## Common Patterns

**Async Testing:**
```python
# Mark async test functions with decorator (line 77)
@pytest.mark.asyncio
async def test_delete_200_ok(monkeypatch, headers_base, aiohttp_session_stub):
    # ... test code
    await consumer.process_message(msg, aiohttp_session_stub)
    assert call_log.get("called") is True
```

**Error Testing:**
```python
# Test for exception raising (lines 117-119)
with pytest.raises(RuntimeError) as ei:
    await consumer.process_message(msg, aiohttp_session_stub)
assert "RAG delete 5xx" in str(ei.value)
```

**Call Tracking:**
```python
# Track function calls via dictionary mutation (lines 79-90)
call_log = {}

async def fake_delete(session, rag, partition, file_id):
    call_log["called"] = True
    return FakeResp(200, text_data="OK")

monkeypatch.setattr(consumer, "rag_delete", fake_delete)
# ... run test
assert call_log.get("called") is True
```

**State Verification:**
```python
# Verify function arguments passed (lines 166-177)
calls = {"upsert": None}

async def fake_upsert(session, msg, file_bytes, is_new):
    calls["upsert"] = {
        "file_bytes": file_bytes,
        "is_new": is_new,
        "msg": msg,
    }

# ... run test
assert calls["upsert"]["is_new"] is True
assert calls["upsert"]["file_bytes"] == body
```

## Test Coverage by Area

**Tested:**
- Retry queue selection: `test_next_retry_routing_key_ok()`
- DELETE success path (200, 404): `test_delete_200_ok()`, `test_delete_404_ok()`
- DELETE error handling (5xx, 4xx): `test_delete_5xx_retry_exception()`
- UPSERT GET errors (5xx, 4xx): `test_upsert_get_5xx_raises_runtimeerror()`, `test_upsert_get_4xx_raises_exception()`
- UPSERT new file flow (404→POST): `test_upsert_new_on_404_triggers_post()`
- UPSERT update flow (200 with md5 change→PUT): `test_upsert_update_on_md5_change_triggers_put()`
- UPSERT skip optimization (same md5): `test_upsert_same_md5_skips()`

**Not Tested:**
- `build_metadata()` function logic
- `build_headers()` in producer (only tested indirectly via process_message)
- `rag_upsert()` HTTP multipart form building
- `get_producer_file()` file download logic
- `declare_topology()` queue/exchange setup
- `republish_to_retry()` DLQ routing logic
- Main event loop and aio_pika integration
- Producer CLI argument parsing
- Error cases like missing content or invalid actions (some implicit via validation)

---

*Testing analysis: 2026-02-17*
