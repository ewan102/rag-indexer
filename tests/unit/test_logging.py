"""Tests for sensitive-field scrubbing in structured logging."""

from rag_indexer.logging import drop_sensitive_keys, _redact_url


def test_redact_url_masks_secret_after_downloads():
    url = "http://cozy.localhost/files/downloads/S3CR3T/myfile.pdf"
    assert _redact_url(url) == "http://cozy.localhost/files/downloads/<redacted>"


def test_redact_url_without_downloads_marker_keeps_host():
    assert _redact_url("http://example.com/elsewhere/file.bin") == "http://example.com/<redacted>"


def test_redact_url_without_scheme_is_fully_redacted():
    assert _redact_url("not-a-url") == "<redacted>"


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


# ------------------------
# callback_token: must never reach the logs
# ------------------------
def test_drop_sensitive_keys_drops_callback_token():
    event = {
        "event": "upsert_callback_token_forwarded",
        "callback_token": "cozy-secret-token",
        "callback_url": "https://cozy.example/ai/index/status",
    }
    out = drop_sensitive_keys(None, None, event)
    assert "callback_token" not in out
    # URL alongside it is redacted, not dropped
    assert out["callback_url"] == "https://cozy.example/<redacted>"


def test_drop_sensitive_keys_drops_callback_token_nested_in_dict():
    event = {"event": "x", "message": {"callback_token": "cozy-secret-token", "file_id": "f1"}}
    out = drop_sensitive_keys(None, None, event)
    assert "callback_token" not in out["message"]
    assert out["message"]["file_id"] == "f1"


def test_callback_token_value_never_appears_in_rendered_event():
    """The raw secret must not survive anywhere in the event dict."""
    event = {"event": "dlq_callback_sent", "callback_token": "S3CR3T-TOKEN", "partition": "p1"}
    out = drop_sensitive_keys(None, None, event)
    assert "S3CR3T-TOKEN" not in str(out)
