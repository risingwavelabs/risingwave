#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HASURA_URL=${HASURA_URL:-http://127.0.0.1:18080}
HASURA_ADMIN_SECRET=${HASURA_ADMIN_SECRET:-rw-hasura}
RW_PG_URL=${RW_PG_URL:-postgres://root@127.0.0.1:4566/dev}
SOURCE_NAME=${SOURCE_NAME:-rw}

compose() {
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" "$@"
}

cleanup() {
  compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require curl
require jq
require psql
require docker

wait_for_http() {
  local url=$1
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $url" >&2
  return 1
}

metadata() {
  curl -fsS \
    -H 'Content-Type: application/json' \
    -H "X-Hasura-Admin-Secret: ${HASURA_ADMIN_SECRET}" \
    -d "$1" \
    "$HASURA_URL/v1/metadata"
}

graphql() {
  curl -fsS \
    -H 'Content-Type: application/json' \
    -H "X-Hasura-Admin-Secret: ${HASURA_ADMIN_SECRET}" \
    -d "$1" \
    "$HASURA_URL/v1/graphql"
}

run_sql() {
  local sql=$1
  curl -fsS \
    -H 'Content-Type: application/json' \
    -H "X-Hasura-Admin-Secret: ${HASURA_ADMIN_SECRET}" \
    -d "$(jq -cn --arg source "$SOURCE_NAME" --arg sql "$sql" '{type:"run_sql", args:{source:$source, sql:$sql, read_only:true}}')" \
    "$HASURA_URL/v2/query"
}

wait_for_source_sql() {
  local sql=$1
  for _ in $(seq 1 30); do
    if run_sql "$sql" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for source SQL readiness: $sql" >&2
  return 1
}

run_sql_retry() {
  local sql=$1
  for _ in $(seq 1 30); do
    if run_sql "$sql"; then
      return 0
    fi
    sleep 1
  done
  echo "timed out running source SQL: $sql" >&2
  return 1
}

echo "[1/7] start Hasura + metadata Postgres"
compose down -v >/dev/null 2>&1 || true
compose up -d >/dev/null
wait_for_http "$HASURA_URL/healthz"

echo "[2/7] prepare RisingWave objects"
psql "$RW_PG_URL" -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/setup.sql" >/dev/null

echo "[3/7] register RisingWave as Hasura source"
metadata "$(jq -cn --arg source "$SOURCE_NAME" '{type:"pg_add_source", args:{name:$source, configuration:{connection_info:{database_url:{from_env:"RW_DATABASE_URL"}, use_prepared_statements:false}}, replace_configuration:true}}')" >/dev/null
wait_for_source_sql "select 1;"

echo "[4/7] verify Hasura can read Hasura-critical pg_catalog metadata from RisingWave"
run_sql_retry "select pg_catalog.pg_relation_is_updatable('hasura_defaults'::regclass, true) as updatable_mask;" \
  | jq -e '.result[1][0] == "28"' >/dev/null
run_sql_retry "select contype, conkey::text, confkey::text, confdeltype, confupdtype from pg_catalog.pg_constraint where conrelid = 'hasura_child'::regclass and contype = 'f';" \
  | jq -e '.result[1][0] == "f" and .result[1][3] == "c" and .result[1][4] == "n"' >/dev/null
run_sql_retry "select proname, prokind, proretset from pg_catalog.pg_proc where proname = 'hasura_add';" \
  | jq -e '.result[1][0] == "hasura_add" and .result[1][1] == "f" and .result[1][2] == "f"' >/dev/null
run_sql_retry "select pg_catalog.pg_get_functiondef(oid) from pg_catalog.pg_proc where proname = 'hasura_add';" \
  | jq -e '.result[1][0] | contains("CREATE FUNCTION public.hasura_add")' >/dev/null

echo "[5/7] track tables"
metadata "$(jq -cn --arg source "$SOURCE_NAME" '{type:"bulk", args:[
  {type:"pg_track_table", args:{source:$source, table:{schema:"public", name:"hasura_parent"}}},
  {type:"pg_track_table", args:{source:$source, table:{schema:"public", name:"hasura_child"}}},
  {type:"pg_track_table", args:{source:$source, table:{schema:"public", name:"hasura_defaults"}}},
  {type:"pg_track_table", args:{source:$source, table:{schema:"public", name:"hasura_defaults_view"}}}
]}')" >/dev/null

echo "[6/7] verify generated GraphQL schema shape"
graphql '{"query":"query { __type(name: \"hasura_defaults_insert_input\") { inputFields { name type { kind name ofType { kind name ofType { kind name } } } } } }"}' \
  | jq -e '
    .data.__type.inputFields as $fields
    | any($fields[]; .name == "id" and .type.kind == "SCALAR" and .type.name == "Int")
    and any($fields[]; .name == "required_v" and .type.kind == "SCALAR" and .type.name == "Int")
    and any($fields[]; .name == "defaulted_v" and .type.kind == "SCALAR" and .type.name == "Int")
    and any($fields[]; .name == "optional_v" and .type.kind == "SCALAR" and .type.name == "Int")
    and all($fields[]; .name != "generated_v")
  ' >/dev/null

echo "[7/7] run GraphQL query + mutation path"
graphql '{"query":"mutation { insert_hasura_parent_one(object:{id: 1, name: \"p1\"}) { id name } insert_hasura_child_one(object:{id: 10, parent_id: 1, note: \"c1\"}) { id parent_id } insert_hasura_defaults_one(object:{id: 100, required_v: 7}) { id required_v defaulted_v generated_v } }"}' \
  | jq -e '.data.insert_hasura_defaults_one.defaulted_v == 42 and .data.insert_hasura_defaults_one.generated_v == 8' >/dev/null
graphql '{"query":"query { hasura_defaults(where:{id:{_eq:100}}) { id required_v defaulted_v generated_v } hasura_defaults_view(where:{id:{_eq:100}}) { id required_v defaulted_v generated_v } }"}' \
  | jq -e '.data.hasura_defaults[0].generated_v == 8 and .data.hasura_defaults_view[0].generated_v == 8' >/dev/null
graphql '{"query":"mutation { update_hasura_defaults(where:{id:{_eq:100}}, _set:{required_v: 9}) { affected_rows } delete_hasura_child(where:{id:{_eq:10}}) { affected_rows } }"}' \
  | jq -e '.data.update_hasura_defaults.affected_rows == 1 and .data.delete_hasura_child.affected_rows == 1' >/dev/null

echo "Hasura compatibility smoke test passed against RisingWave."
