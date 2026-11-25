"""Daily batch: raw archive -> PostgreSQL -> dbt (day, week and month rankings).

Runs at 01:00 UTC and loads the two previous calendar days (UTC):
- yesterday, now complete apart from the last archiver flush (every 5 minutes)
  and edits still in flight;
- the day before, a second time, to pick up what arrived after the previous
  run (late events, a producer replay after an outage).
load_postgres.py replaces a range instead of adding to it, so loading a day
twice is safe. dbt then picks up the (re)loaded rows (fact_edits is
incremental on loaded_at), rebuilds the rankings and runs the tests; a failed
test fails the run.

Each task runs in its own container (DockerOperator) from the project's
images, so Airflow needs neither Java, Spark nor dbt, and the jobs run exactly
as they do by hand (docker compose run --rm load-edits / dbt).

max_active_runs=1: runs share the load table raw.edits_load, and a catch-up
must load days in order.
"""
import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

NETWORK = os.environ.get("WIKIHEATMAP_NETWORK", "wikiheatmap_default")
JOB_ENV = {
    "PG_HOST": "postgres",
    "PG_USER": os.environ.get("PG_USER", "wikiheatmap"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD", "wikiheatmap-secret"),
    "S3_ENDPOINT": "http://seaweedfs:8333",
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", "wikiheatmap"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", "wikiheatmap-secret"),
}
DOCKER = {
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": NETWORK,
    "environment": JOB_ENV,
    "auto_remove": "success",
    # Airflow runs in a container: a temp dir bind-mounted from it would not
    # exist on the Docker host.
    "mount_tmp_dir": False,
}

with DAG(
    dag_id="wikiheatmap_daily",
    # A trigger timetable: the run at 01:00 UTC on day D has ds = D.
    schedule=CronTriggerTimetable("0 1 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 25, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["wikiheatmap"],
) as dag:
    load_edits = DockerOperator(
        task_id="load_edits",
        image="wikiheatmap-spark",
        # [D-2, D): yesterday and the day before.
        # Reads the Parquet raw archive (default paths of load_postgres.py).
        command=["python", "load_postgres.py",
                 "{{ macros.ds_add(ds, -2) }} 00:00:00", "{{ ds }} 00:00:00"],
        **DOCKER,
    )
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="wikiheatmap-dbt",
        command=["build"],
        **DOCKER,
    )
    load_edits >> dbt_build
