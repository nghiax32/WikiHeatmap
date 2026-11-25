"""PostgreSQL connection settings and a JDBC write helper, shared by the
batch loader (load_postgres.py) and the streaming query (stream_top_pages.py).
"""
import os

from pyspark.sql import DataFrame

PG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "dbname": os.environ.get("PG_DB", "wikiheatmap"),
    "user": os.environ.get("PG_USER", "wikiheatmap"),
    "password": os.environ.get("PG_PASSWORD", "wikiheatmap-secret"),
}


def jdbc_append(df: DataFrame, table: str) -> None:
    """Append df to an existing table. Callers write to a load table and move
    rows on in a psycopg2 transaction, so a failed write changes nothing."""
    (df.write.format("jdbc")
        .option("url", f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
                       "?reWriteBatchedInserts=true")
        .option("dbtable", table)
        .option("user", PG["user"]).option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10_000)
        .mode("append")
        .save())
