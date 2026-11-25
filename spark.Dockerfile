# Image for the Spark jobs (analysis/): streaming queries and batch loads.
# Spark runs in local mode inside the container. Dependencies come from uv.lock.
FROM python:3.12.16-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv==0.13.5

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache --no-default-groups --group spark --no-install-project
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY analysis/ analysis/
# Download the Spark packages (S3A, Kafka, JDBC) into the image, not at every start.
RUN cd analysis && python -c "from load_edits import get_spark; get_spark('warm-up').stop()"
WORKDIR /app/analysis
