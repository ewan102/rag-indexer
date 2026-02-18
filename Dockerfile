# --- Build stage: install dependencies with uv ---
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

# Install dependencies first (layer caching: only re-run when lockfile changes)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Copy application source and install the project itself
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# --- Runtime stage: slim image without uv ---
FROM python:3.12-slim

# Create non-root user with fixed UID/GID
RUN groupadd --system --gid 999 app \
 && useradd --system --gid 999 --uid 999 --create-home app

# Copy the entire app (including .venv) from builder
COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:$PATH"

USER app
WORKDIR /app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

CMD ["python", "consumer.py"]
