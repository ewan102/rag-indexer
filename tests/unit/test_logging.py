"""Tests for sensitive-field scrubbing in structured logging."""

from rag_indexer.logging import drop_sensitive_keys, _redact_url


def test_redact_url_masks_secret_after_downloads():
    url = "http://cozy.localhost/files/downloads/S3CR3T/myfile.pdf"
    assert _redact_url(url) == "http://cozy.localhost/files/downloads/<redacted>"


def test_redact_url_without_marker_is_fully_redacted():
    assert _redact_url("http://example.com/elsewhere/file.bin") == "<redacted>"


def test_redact_url_non_string_passthrough():
    assert _redact_url(None) is None


def test_drop_sensitive_keys_redacts_file_url_and_drops_credentials():
    event = {
        "event": "file_download",
        "file_url": "http://c/files/downloads/S3CR3T/f.pdf",
        "rag_api_key": "key",
        "authorization": "Bearer x",
    }
    out = drop_sensitive_keys(None, None, event)
    assert out["file_url"] == "http://c/files/downloads/<redacted>"
    assert "rag_api_key" not in out
    assert "authorization" not in out


def test_drop_sensitive_keys_redacts_file_url_nested_in_dict():
    event = {"event": "x", "content": {"file_url": "http://c/files/downloads/S3CR3T/f.pdf"}}
    out = drop_sensitive_keys(None, None, event)
    assert out["content"]["file_url"] == "http://c/files/downloads/<redacted>"
