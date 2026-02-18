import pytest
from aiormq import AMQPConnectionError

from rag_indexer.main import main


# ------------------------
# BUGF-03: Exit code on connection failure
# ------------------------
@pytest.mark.asyncio
async def test_main_exits_with_nonzero_on_connection_failure(monkeypatch):
    async def fake_connect_robust(url):
        raise AMQPConnectionError("Connection refused")

    monkeypatch.setattr("aio_pika.connect_robust", fake_connect_robust)

    with pytest.raises(SystemExit) as exc_info:
        await main()
    assert exc_info.value.code == 1
