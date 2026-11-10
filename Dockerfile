# Image for the Python services (producer, raw archiver). Dependencies come from uv.lock.
FROM python:3.12.16-slim

RUN pip install --no-cache-dir uv==0.13.5

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY scripts/ scripts/
