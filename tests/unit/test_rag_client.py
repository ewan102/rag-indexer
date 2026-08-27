import pytest

from rag_indexer.models import IndexMessage, RagConn, ContentSpec
from rag_indexer.rag_client import get_producer_file, build_metadata, rag_upsert
from tests.conftest import FakeContextResp, FakeSession


# ------------------------
# BUGF-01: File download reads inside context manager
# ------------------------
@pytest.mark.asyncio
async def test_get_producer_file_reads_inside_context():
    expected_bytes = b"file-content-here"
    fake_resp = FakeContextResp(status=200, body=expected_bytes)
    fake_session = FakeSession(fake_resp)

    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        content=ContentSpec(file_url="http://example.com/file.bin"),
    )
    result = await get_producer_file(fake_session, msg)
    assert result == expected_bytes


# ------------------------
# BUGF-02: Metadata merge
# ------------------------
def test_build_metadata_merges_app_metadata():
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        doctype="pdf",
        app_metadata={"custom_key": "custom_value", "author": "test"},
    )
    meta = build_metadata(msg)
    assert meta["custom_key"] == "custom_value"
    assert meta["author"] == "test"
    assert meta["version"] == "v1"
    assert meta["doctype"] == "pdf"


def test_build_metadata_without_app_metadata():
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        doctype="pdf",
    )
    meta = build_metadata(msg)
    assert meta["version"] == "v1"
    assert meta["doctype"] == "pdf"
    # No extra keys beyond the base fields
    assert "custom_key" not in meta


# ------------------------
# NEW: File download via self-authenticating file_url
# ------------------------
@pytest.mark.asyncio
async def test_get_producer_file_no_auth_header():
    """get_producer_file downloads from file_url without any Authorization header.

    The file_url is self-authenticating (secret in the path), so no bearer is
    ever sent. This verifies the function returns content correctly.
    """
    expected_bytes = b"secure-file-content"
    fake_resp = FakeContextResp(status=200, body=expected_bytes)
    fake_session = FakeSession(fake_resp)

    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        content=ContentSpec(
            file_url="http://example.com/files/downloads/sekret/secure.bin",
        ),
    )
    result = await get_producer_file(fake_session, msg)
    assert result == expected_bytes


# ------------------------
# NEW: Metadata edge cases (TEST-03, TEST-04)
# ------------------------
def test_build_metadata_app_metadata_overrides_base_keys():
    """app_metadata containing 'version' should override the base version field."""
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        doctype="pdf",
        app_metadata={"version": "custom_version"},
    )
    meta = build_metadata(msg)
    assert meta["version"] == "custom_version"


def test_build_metadata_empty_dict_app_metadata():
    """app_metadata as empty dict should produce same result as no app_metadata."""
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        doctype="pdf",
        app_metadata={},
    )
    meta = build_metadata(msg)
    assert meta["version"] == "v1"
    assert meta["doctype"] == "pdf"
    assert meta["datetime"] == ""
    assert len(meta) == 3  # only base fields


def test_build_metadata_none_values_in_app_metadata():
    """app_metadata with None values should preserve them in the merge."""
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
        version="v1",
        app_metadata={"extra": None},
    )
    meta = build_metadata(msg)
    assert meta["extra"] is None
    assert meta["version"] == "v1"


def test_build_metadata_no_version_no_md5sum():
    """Both version and md5sum are None -- meta['version'] should fallback to empty string."""
    msg = IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        rag=RagConn(base_url="http://rag:8000", api_key="key"),
    )
    meta = build_metadata(msg)
    assert meta["version"] == ""


# ------------------------
# callback_token: multipart field to OpenRAG, never in the URL
# ------------------------
class _CapturingSession:
    """Fake ClientSession recording the rag_upsert request."""

    def __init__(self, status: int = 201):
        self._status = status
        self.request_call = None

    def get(self, url, headers=None, timeout=None):
        return FakeContextResp(status=200, body=b"file-bytes")

    def request(self, method, url, *, data=None, params=None, headers=None, timeout=None):
        self.request_call = {
            "method": method,
            "url": url,
            "form": {
                opts["name"]: value
                for opts, _hdrs, value in data._fields
                if "filename" not in opts
            },
            "params": params,
            "headers": headers,
        }
        return _FakeRequestResp(self._status)


class _FakeRequestResp:
    def __init__(self, status: int):
        self.status = status

    async def text(self):
        return ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def _upsert_msg(**kwargs) -> IndexMessage:
    return IndexMessage(
        action="upsert",
        partition="p1",
        file_id="f1",
        name="doc.txt",
        rag=RagConn(base_url="http://rag:8000", api_key="rag-key"),
        content=ContentSpec(file_url="http://cozy/files/downloads/sekret/doc.txt"),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_rag_upsert_forwards_callback_token_as_form_field():
    session = _CapturingSession()
    msg = _upsert_msg(
        callback_url="https://cozy.example/ai/index/status",
        callback_token="cozy-secret-token",
    )
    await rag_upsert(session, msg, is_new=True)

    call = session.request_call
    assert call["form"]["callback_token"] == "cozy-secret-token"
    assert call["form"]["callback_url"] == "https://cozy.example/ai/index/status"


@pytest.mark.asyncio
async def test_rag_upsert_never_puts_callback_token_in_query_string():
    """The token must not land in params, the URL, or an auth header."""
    session = _CapturingSession()
    msg = _upsert_msg(callback_token="cozy-secret-token")
    await rag_upsert(session, msg, is_new=True)

    call = session.request_call
    assert "cozy-secret-token" not in call["url"]
    assert "cozy-secret-token" not in str(call["params"])
    # Authorization must stay the OpenRAG api_key
    assert call["headers"]["Authorization"] == "Bearer rag-key"
    assert "cozy-secret-token" not in str(call["headers"])


@pytest.mark.asyncio
async def test_rag_upsert_omits_callback_token_when_absent():
    """Optional field: no callback_token means no such form field."""
    session = _CapturingSession()
    await rag_upsert(session, _upsert_msg(callback_url="https://cozy.example/cb"), is_new=True)

    call = session.request_call
    assert "callback_token" not in call["form"]
    assert call["form"]["callback_url"] == "https://cozy.example/cb"
