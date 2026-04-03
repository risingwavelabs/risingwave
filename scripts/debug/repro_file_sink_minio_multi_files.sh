#!/usr/bin/env bash
set -euo pipefail

# One-click repro for "multiple files within 1 second" on file sink (S3/MinIO).
# By default this script will restart env via:
#   ./risedev k || true
#   ./risedev clean-data
#   ./risedev d "${RW_PROFILE}"
# Set SKIP_ENV_SETUP=1 to reuse an existing cluster.

ROWS=${ROWS:-16000}
MAX_ROW_COUNT=${MAX_ROW_COUNT:-1}
ROLLOVER_SECONDS=${ROLLOVER_SECONDS:-3600}
SINK_PARALLELISM=${SINK_PARALLELISM:-1}
WAIT_TIMEOUT_SEC=${WAIT_TIMEOUT_SEC:-120}
POLL_INTERVAL_SEC=${POLL_INTERVAL_SEC:-2}

RW_PROFILE=${RW_PROFILE:-wcy}
SKIP_ENV_SETUP=${SKIP_ENV_SETUP:-0}
RW_READY_TIMEOUT_SEC=${RW_READY_TIMEOUT_SEC:-180}

S3_BUCKET=${S3_BUCKET:-hummock001}
S3_ENDPOINT=${S3_ENDPOINT:-http://hummock001.127.0.0.1:9301}
S3_REGION=${S3_REGION:-custom}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-hummockadmin}
S3_SECRET_KEY=${S3_SECRET_KEY:-hummockadmin}

# Optional direct MinIO listing endpoint (for Python MinIO SDK), host:port without schema.
MINIO_ENDPOINT=${MINIO_ENDPOINT:-127.0.0.1:9301}

RUN_ID=${RUN_ID:-$(date +%s)}
SOURCE_TABLE=${SOURCE_TABLE:-t_file_sink_repro_${RUN_ID}}
SINK_NAME=${SINK_NAME:-s_file_sink_repro_${RUN_ID}}
READBACK_TABLE=${READBACK_TABLE:-t_file_sink_readback_${RUN_ID}}
FILE_SOURCE_NAME=${FILE_SOURCE_NAME:-src_file_sink_repro_${RUN_ID}}
FILE_SOURCE_MV_NAME=${FILE_SOURCE_MV_NAME:-mv_file_sink_repro_${RUN_ID}}
S3_PATH_PREFIX=${S3_PATH_PREFIX:-test_file_sink_repro/${RUN_ID}}

KEEP_ARTIFACTS=${KEEP_ARTIFACTS:-1}

psql() {
  ./risedev psql -d dev -c "$1"
}

query_count() {
  local sql="$1"
  ./risedev psql -d dev -At -c "${sql}" \
    | grep -Eo '^[0-9]+$' \
    | tail -n 1
}

wait_for_rw_ready() {
  local start_ts now_ts elapsed
  start_ts=$(date +%s)
  while true; do
    if ./risedev psql -d dev -At -c "SELECT 1;" >/dev/null 2>&1; then
      echo "RisingWave is ready."
      return 0
    fi
    now_ts=$(date +%s)
    elapsed=$((now_ts - start_ts))
    if (( elapsed >= RW_READY_TIMEOUT_SEC )); then
      echo "Timeout: RisingWave is not ready within ${RW_READY_TIMEOUT_SEC}s"
      return 1
    fi
    sleep 2
  done
}

echo "== File sink repro config =="
echo "ROWS=$ROWS"
echo "MAX_ROW_COUNT=$MAX_ROW_COUNT"
echo "ROLLOVER_SECONDS=$ROLLOVER_SECONDS"
echo "SINK_PARALLELISM=$SINK_PARALLELISM"
echo "RW_PROFILE=$RW_PROFILE"
echo "SKIP_ENV_SETUP=$SKIP_ENV_SETUP"
echo "S3 path=s3://$S3_BUCKET/$S3_PATH_PREFIX/"
echo

if [[ "${SKIP_ENV_SETUP}" != "1" ]]; then
  echo "[0/7] Restart environment with MinIO profile"
  ./risedev k >/dev/null 2>&1 || true
  ./risedev clean-data
  ./risedev d "${RW_PROFILE}"
else
  echo "[0/7] Skip environment setup (SKIP_ENV_SETUP=1)"
fi

echo "[1/7] Wait for RisingWave to be ready"
wait_for_rw_ready

echo "[2/7] Create source table and preload data (for backfill)"
psql "CREATE TABLE ${SOURCE_TABLE} (id INT, payload VARCHAR);"
psql "INSERT INTO ${SOURCE_TABLE} SELECT g, 'payload-' || g::VARCHAR FROM generate_series(1, ${ROWS}) AS g;"

echo "[3/7] Create S3 file sink with tiny max_row_count"
psql "CREATE SINK ${SINK_NAME} AS SELECT id, payload FROM ${SOURCE_TABLE} WITH (\
  connector = 's3',\
  s3.region_name = '${S3_REGION}',\
  s3.bucket_name = '${S3_BUCKET}',\
  s3.credentials.access = '${S3_ACCESS_KEY}',\
  s3.credentials.secret = '${S3_SECRET_KEY}',\
  s3.endpoint_url = '${S3_ENDPOINT}',\
  s3.path = '${S3_PATH_PREFIX}/',\
  type = 'append-only',\
  force_append_only = 'true',\
  max_row_count = '${MAX_ROW_COUNT}',\
  rollover_seconds = '${ROLLOVER_SECONDS}'\
) FORMAT PLAIN ENCODE PARQUET(force_append_only='true');"
psql "ALTER SINK ${SINK_NAME} SET PARALLELISM = ${SINK_PARALLELISM};"

echo "[4/9] Create readback table on S3 path"
psql "CREATE TABLE ${READBACK_TABLE} (id INT, payload VARCHAR) WITH (\
  connector = 's3',\
  match_pattern = '${S3_PATH_PREFIX}/*.parquet',\
  refresh.interval.sec = 1,\
  s3.region_name = '${S3_REGION}',\
  s3.bucket_name = '${S3_BUCKET}',\
  s3.credentials.access = '${S3_ACCESS_KEY}',\
  s3.credentials.secret = '${S3_SECRET_KEY}',\
  s3.endpoint_url = '${S3_ENDPOINT}'\
) FORMAT PLAIN ENCODE PARQUET;"

echo "[5/9] Create file source + mv(count) on the same path"
psql "CREATE SOURCE ${FILE_SOURCE_NAME} (id INT, payload VARCHAR) WITH (\
  connector = 's3',\
  match_pattern = '${S3_PATH_PREFIX}/*.parquet',\
  refresh.interval.sec = 1,\
  s3.region_name = '${S3_REGION}',\
  s3.bucket_name = '${S3_BUCKET}',\
  s3.credentials.access = '${S3_ACCESS_KEY}',\
  s3.credentials.secret = '${S3_SECRET_KEY}',\
  s3.endpoint_url = '${S3_ENDPOINT}'\
) FORMAT PLAIN ENCODE PARQUET;"
psql "CREATE MATERIALIZED VIEW ${FILE_SOURCE_MV_NAME} AS SELECT COUNT(*) AS cnt FROM ${FILE_SOURCE_NAME};"

echo "[6/9] Wait until readback table catches up"
start_ts=$(date +%s)
while true; do
  got="$(query_count "SELECT COUNT(*) FROM ${READBACK_TABLE};" || true)"
  got="${got:-0}"
  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))
  echo "  elapsed=${elapsed}s readback_count=${got}/${ROWS}"
  if [[ "${got}" == "${ROWS}" ]]; then
    break
  fi
  if (( elapsed >= WAIT_TIMEOUT_SEC )); then
    echo "Timeout: readback did not reach expected count within ${WAIT_TIMEOUT_SEC}s"
    break
  fi
  sleep "${POLL_INTERVAL_SEC}"
done

echo "[7/9] Wait until file source mv(count) catches up"
start_ts=$(date +%s)
while true; do
  got_src_mv="$(query_count "SELECT cnt FROM ${FILE_SOURCE_MV_NAME};" || true)"
  got_src_mv="${got_src_mv:-0}"
  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))
  echo "  elapsed=${elapsed}s source_mv_count=${got_src_mv}/${ROWS}"
  if [[ "${got_src_mv}" == "${ROWS}" ]]; then
    break
  fi
  if (( elapsed >= WAIT_TIMEOUT_SEC )); then
    echo "Timeout: source mv count did not reach expected count within ${WAIT_TIMEOUT_SEC}s"
    break
  fi
  sleep "${POLL_INTERVAL_SEC}"
done

echo "[8/9] Final counts"
src_cnt="$(query_count "SELECT COUNT(*) FROM ${SOURCE_TABLE};" || true)"
rb_cnt="$(query_count "SELECT COUNT(*) FROM ${READBACK_TABLE};" || true)"
src_mv_cnt="$(query_count "SELECT cnt FROM ${FILE_SOURCE_MV_NAME};" || true)"
src_cnt="${src_cnt:-unknown}"
rb_cnt="${rb_cnt:-unknown}"
src_mv_cnt="${src_mv_cnt:-unknown}"
echo "  source_count=${src_cnt}"
echo "  readback_count=${rb_cnt}"
echo "  source_mv_count=${src_mv_cnt}"

cat <<MSG
[9/9] Optional per-file detail (needs python deps: minio + pyarrow)
- If deps are available, the script will print object names and row counts per parquet file.
- This helps estimate how many rows each flush/chunk produced.
MSG

python3 - <<PY || true
import io
import sys
from collections import defaultdict

try:
    from minio import Minio
    import pyarrow.parquet as pq
except Exception as e:
    print(f"Skip per-file detail: missing dependency ({e})")
    sys.exit(0)

bucket = "${S3_BUCKET}"
minio_endpoint = "${MINIO_ENDPOINT}"
access_key = "${S3_ACCESS_KEY}"
secret_key = "${S3_SECRET_KEY}"
prefix = "${S3_PATH_PREFIX}/"

client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False)
objects = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True))
parquet_objects = [o.object_name for o in objects if o.object_name.endswith('.parquet')]
parquet_objects.sort()

print(f"Found parquet files: {len(parquet_objects)}")
if not parquet_objects:
    sys.exit(0)

total_rows = 0
by_second = defaultdict(int)
for obj in parquet_objects:
    resp = client.get_object(bucket, obj)
    data = resp.read()
    resp.close()
    resp.release_conn()

    table = pq.read_table(io.BytesIO(data))
    n = table.num_rows
    total_rows += n

    # Expected name pattern: <prefix>/<uuid>_<secs>_<seq>.parquet
    base = obj.rsplit('/', 1)[-1]
    parts = base.split('_')
    if len(parts) >= 3:
        sec = parts[-2]
        by_second[sec] += 1

    print(f"  {obj}\trows={n}")

print(f"Total rows from parquet files: {total_rows}")
print("Files grouped by timestamp(second):")
for sec in sorted(by_second):
    print(f"  sec={sec}\tfiles={by_second[sec]}")
PY

if [[ "${KEEP_ARTIFACTS}" == "0" ]]; then
  echo "Cleaning SQL objects (KEEP_ARTIFACTS=0)"
  psql "DROP MATERIALIZED VIEW IF EXISTS ${FILE_SOURCE_MV_NAME};"
  psql "DROP SOURCE IF EXISTS ${FILE_SOURCE_NAME};"
  psql "DROP TABLE IF EXISTS ${READBACK_TABLE};"
  psql "DROP SINK IF EXISTS ${SINK_NAME};"
  psql "DROP TABLE IF EXISTS ${SOURCE_TABLE};"
else
  cat <<INFO
Artifacts kept:
- source table: ${SOURCE_TABLE}
- sink: ${SINK_NAME}
- readback table: ${READBACK_TABLE}
- file source: ${FILE_SOURCE_NAME}
- file source mv: ${FILE_SOURCE_MV_NAME}
- s3 prefix: s3://${S3_BUCKET}/${S3_PATH_PREFIX}/
INFO
fi
