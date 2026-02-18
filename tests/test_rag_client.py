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
