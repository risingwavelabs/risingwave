#!/usr/bin/env bash
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

mode="${1:?expected deny or allow}"
case "$mode" in
    deny|allow) ;;
    *) echo "expected deny or allow" >&2; exit 1 ;;
esac

connection=(-X -qAt -v ON_ERROR_STOP=1 -h "${SLT_HOST:?}" -p "${SLT_PORT:?}" -d "${SLT_DB:?}")
# Resolve IDs as root so catalog visibility cannot turn a denied call into an empty query.
targets=$(psql "${connection[@]}" -U root -c "
SELECT 'table', id FROM rw_catalog.rw_tables WHERE name = 'warmup_test'
UNION ALL
SELECT 'index', id FROM rw_catalog.rw_indexes WHERE name = 'warmup_idx'
UNION ALL
SELECT 'internal', id FROM rw_catalog.rw_internal_tables
WHERE job_id = (SELECT id FROM rw_catalog.rw_materialized_views WHERE name = 'warmup_agg');")

seen_table=false
seen_index=false
seen_internal=false
while IFS='|' read -r kind table_id; do
    case "$kind" in
        table) seen_table=true ;;
        index) seen_index=true ;;
        internal) seen_internal=true ;;
        *) echo "unexpected warm-up target: $kind" >&2; exit 1 ;;
    esac
    if output=$(PGPASSWORD=password psql "${connection[@]}" -U warmup_user -c \
        "SET batch_expr_strict_mode = true; SELECT rw_warmup($table_id) >= 0;" 2>&1); then
        if [[ "$mode" != allow || "$output" != t ]]; then
            echo "unexpected warm-up success for $kind $table_id: $output" >&2
            exit 1
        fi
    elif [[ "$mode" != deny || "$output" != *"only the table owner or a superuser"* ]]; then
        echo "unexpected warm-up error for $kind $table_id: $output" >&2
        exit 1
    fi
done <<< "$targets"

if [[ "$seen_table" != true || "$seen_index" != true || "$seen_internal" != true ]]; then
    echo "missing table, index, or internal-table coverage" >&2
    exit 1
fi
