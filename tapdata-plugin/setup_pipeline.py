"""
Set up a Tapdata pipeline: PostgreSQL -> RisingWave.
Uses the RisingWave Tapdata connector in streaming mode.
"""
import builtins
import os
import sys

builtins.get_ipython = lambda: None
sys.path.insert(0, '/opt/homebrew/lib/python3.13/site-packages')

from tapflow.lib.login import login_with_access_code
from tapflow.lib.data_pipeline.data_source import DataSource
from tapflow.lib.data_pipeline.pipeline import Pipeline, Flow
from tapflow.lib.op_object import show_connectors, show_connections, show_jobs

SERVER = os.environ.get("TAPDATA_SERVER", "127.0.0.1:3030")
ACCESS_CODE = os.environ.get("TAPDATA_ACCESS_CODE", "3324cfdf-7d3e-4792-bd32-571638d4562f")
JOB_NAME = os.environ.get("TAPDATA_JOB_NAME", "pg_to_risingwave_native")
PG_CONNECTION = os.environ.get("TAPDATA_PG_CONNECTION", "PG_Source_postgres")
RW_CONNECTION = os.environ.get("TAPDATA_RW_CONNECTION", "RW_Native_dev")
PG_HOST = os.environ.get("TAPDATA_PG_HOST", "host.docker.internal")
PG_PORT = int(os.environ.get("TAPDATA_PG_PORT", "5432"))
PG_DATABASE = os.environ.get("TAPDATA_PG_DATABASE", "postgres")
PG_SCHEMA = os.environ.get("TAPDATA_PG_SCHEMA", "public")
PG_USER = os.environ.get("TAPDATA_PG_USER", "william")
PG_PASSWORD = os.environ.get("TAPDATA_PG_PASSWORD", "")
RW_HOST = os.environ.get("TAPDATA_RW_HOST", "host.docker.internal")
RW_PORT = int(os.environ.get("TAPDATA_RW_PORT", "4566"))
RW_DATABASE = os.environ.get("TAPDATA_RW_DATABASE", "dev")
RW_SCHEMA = os.environ.get("TAPDATA_RW_SCHEMA", "public")
RW_USER = os.environ.get("TAPDATA_RW_USER", "root")
RW_PASSWORD = os.environ.get("TAPDATA_RW_PASSWORD", "")
TABLE = os.environ.get("TAPDATA_TABLE", "orders")

print("=== Tapdata Pipeline Setup: PostgreSQL -> RisingWave (native connector) ===\n")

print("[1/4] Logging in...")
ok = login_with_access_code(SERVER, ACCESS_CODE, interactive=False)
if not ok:
    print("ERROR: Login failed!")
    sys.exit(1)
print("      OK\n")

show_connectors(quiet=True)
show_connections(quiet=True)
show_jobs(quiet=True)

from tapflow.lib.cache import client_cache

# Find the risingwave connector
rw_conn_type = None
for k in client_cache.get("connectors", {}):
    if "risingwave" in k.lower():
        rw_conn_type = k
        break

if not rw_conn_type:
    available = list(client_cache.get("connectors", {}).keys())
    print(f"ERROR: RisingWave connector not found. Available: {available}")
    sys.exit(1)
print(f"[1/4] Using RisingWave connector type: '{rw_conn_type}'\n")

# Find postgres connector for source
pg_conn_type = None
for k in client_cache.get("connectors", {}):
    if "postgres" in k.lower():
        pg_conn_type = k
        break
print(f"[1/4] Using PostgreSQL connector type: '{pg_conn_type}'\n")

print("[2/4] Creating PostgreSQL source connection...")
pg_source = DataSource(
    pg_conn_type,
    PG_CONNECTION,
    {
        "host": PG_HOST,
        "port": PG_PORT,
        "database": PG_DATABASE,
        "schema": PG_SCHEMA,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "logPluginName": "pgoutput",
    }
).type("source").save()
print(f"      Source: {pg_source}\n")

print("[3/4] Creating RisingWave target connection (WebSocket streaming mode)...")
rw_target = DataSource(
    rw_conn_type,
    RW_CONNECTION,
    {
        "host": RW_HOST,
        "port": RW_PORT,
        "database": RW_DATABASE,
        "schema": RW_SCHEMA,
        "user": RW_USER,
        "password": RW_PASSWORD,
        "ingest_mode": "streaming",
        "ingestEndpoint": os.environ.get("TAPDATA_RW_INGEST_ENDPOINT", "ws://host.docker.internal:4560"),
        "webhookSecret": os.environ.get("TAPDATA_RW_WEBHOOK_SECRET", ""),
    }
).type("target").save()
print(f"      Target: {rw_target}\n")

print("[4/4] Creating and starting replication pipeline...")
flow = Flow(JOB_NAME) \
    .read_from(f"{PG_CONNECTION}.{TABLE}") \
    .write_to(f"{RW_CONNECTION}.{TABLE}") \
    .save()
flow.start()
print(f"      Pipeline started: {flow}\n")

print("=== Setup complete! ===")
print("Monitor: http://localhost:3030")
print(f"Verify : psql -h localhost -p {RW_PORT} -U {RW_USER} -d {RW_DATABASE} -c 'SELECT * FROM {TABLE};'")
