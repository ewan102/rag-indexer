from pydantic import BaseModel, field_validator


class RagConn(BaseModel):
    base_url: str
    api_key: str

    @field_validator("base_url")
    @classmethod
    def _strip_trailing_slash(cls, v: str) -> str:
        # Normalize once so URL building never produces '//' (cozy rag.url may
        # carry a trailing slash); applies to every endpoint and the Origin header.
        return v.rstrip("/")


class ContentSpec(BaseModel):
    note_markdown: str | None = None
    file_url: str | None = None


class IndexMessage(BaseModel):
    action: str  # "upsert" | "delete"
    partition: str
    file_id: str
    doctype: str | None = None
    version: str | None = None
    md5sum: str | None = None
    name: str | None = None
    dir_id: str | None = None
    datetime: str | None = None
    content_type: str | None = None
    app_metadata: dict | None = None
    callback_url: str | None = None
    rag: RagConn
    content: ContentSpec | None = None
