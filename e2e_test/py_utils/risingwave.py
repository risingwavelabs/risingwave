import os

import psycopg2


def connect_risingwave(
    database,
    *,
    default_host=None,
    default_port=None,
    user="root",
    password="",
):
    """Create a RisingWave/Postgres connection using the standard env vars."""
    host = os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS", default_host)
    port = os.environ.get("RISEDEV_RW_FRONTEND_PORT", default_port)
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


def fetchone(cursor, query):
    """Execute a query and return the first row."""
    cursor.execute(query)
    return cursor.fetchone()


def fetchall(cursor, query):
    """Execute a query and return all rows."""
    cursor.execute(query)
    return cursor.fetchall()
