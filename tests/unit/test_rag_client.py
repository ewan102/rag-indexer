import pytest

from rag_indexer.models import IndexMessage, RagConn, ContentSpec
from rag_indexer.rag_client import get_producer_file, build_metadata
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
# NEW: File download with bearer token
# ------------------------
@pytest.mark.asyncio
async def test_get_producer_file_with_bearer_token():
    """Verify that get_producer_file works when file_bearer is set.

    The current FakeSession doesn't capture headers, so this test verifies
    the function doesn't crash with a bearer and returns content correctly.
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
            file_url="http://example.com/secure.bin",
            file_bearer="my-bearer-token",
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
