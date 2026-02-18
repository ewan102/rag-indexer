import pytest


# ------------------------
# Helpers & fakes
# ------------------------
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
        # le consumer attend parfois un "read()" pour vider le flux
        return b""


class FakeContextResp:
    """Async context manager that mimics aiohttp response for get_producer_file tests."""
    def __init__(self, status: int, body: bytes):
        self.status = status
        self._body = body

    async def read(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class FakeSession:
    """Fake aiohttp.ClientSession whose .get() returns a FakeContextResp.

    IMPORTANT: .get() must be a regular (synchronous) method, NOT async.
    aiohttp's session.get() returns a context manager object synchronously;
    the async part is the `async with` that calls __aenter__ on it.
    """
    def __init__(self, resp: FakeContextResp):
        self._resp = resp

    def get(self, url, headers=None, timeout=None):
        # Returns the FakeContextResp directly -- it implements __aenter__/__aexit__
        return self._resp


class DummyMessage:
    """Suffisant pour process_message(): expose .body et .headers."""

    def __init__(self, body: bytes, headers: dict):
        self.body = body
        self.headers = headers


@pytest.fixture
def headers_base():
    return {
        "partition": "user-1",
        "file_id": "file-123",
        "rag_base_url": "http://rag:8000",
        "rag_api_key": "secret",
        "content_type": "application/octet-stream",
    }


@pytest.fixture
def aiohttp_session_stub():
    """Le process_message recoit un session aiohttp, mais comme on monkeypatch
    rag_get_file / rag_upsert / rag_delete, cette session ne sera pas reellement utilisee.
    """

    class _S:
        pass

    return _S()
