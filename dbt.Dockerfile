# Image for dbt (dbt/ project), run by the Airflow DAG or by hand:
#   docker compose run --rm dbt build
FROM python:3.12.16-slim

RUN pip install --no-cache-dir uv==0.13.5

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache --no-default-groups --group dbt --no-install-project
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/app/dbt

COPY dbt/ dbt/
WORKDIR /app/dbt
ENTRYPOINT ["dbt"]
