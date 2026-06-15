from typing import Optional

from pydantic import BaseModel


class RagConn(BaseModel):
    base_url: str
    api_key: str


class ContentSpec(BaseModel):
    note_markdown: Optional[str] = None
    file_url: Optional[str] = None
    file_bearer: Optional[str] = None


class IndexMessage(BaseModel):
    action: str  # "upsert" | "delete"
    partition: str
    file_id: str
    doctype: Optional[str] = None
    version: Optional[str] = None
    md5sum: Optional[str] = None
    name: Optional[str] = None
    dir_id: Optional[str] = None
    datetime: Optional[str] = None
    content_type: Optional[str] = None
    app_metadata: Optional[dict] = None
    callback_url: Optional[str] = None
    rag: RagConn
    content: Optional[ContentSpec] = None
