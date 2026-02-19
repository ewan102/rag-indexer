.PHONY: test test-all lint

test:
	uv run pytest tests/unit/ -v

test-all:
	uv run pytest -v

lint:
	uv run ruff check .
