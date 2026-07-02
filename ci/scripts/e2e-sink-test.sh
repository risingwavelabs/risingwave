#!/usr/bin/env bash

set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- e2e, generic sink test"
RUST_LOG="await_tree::future=error" risedev ci-start ci-sink-test

risedev slt './e2e_test/sink/append_only_sink.slt'
risedev slt './e2e_test/sink/blackhole_sink.slt'
risedev slt './e2e_test/sink/file_sink.slt'
risedev slt './e2e_test/sink/license.slt'
risedev slt './e2e_test/sink/rate_limit.slt'
risedev slt './e2e_test/sink/auto_schema_change.slt'
risedev slt './e2e_test/sink/sink_into_table/*.slt'
risedev slt './e2e_test/sink/sink_vector_columns.slt'
risedev slt './e2e_test/sink/force_compaction_sink.slt'

echo "--- e2e, RabbitMQ sink"
export RABBITMQ_AMQP_URL="${RABBITMQ_AMQP_URL:-amqp://guest:guest@rabbitmq-server:5672/%2f}"
export RABBITMQ_MANAGEMENT_URL="${RABBITMQ_MANAGEMENT_URL:-http://rabbitmq-server:15672}"
export RABBITMQ_USERNAME="${RABBITMQ_USERNAME:-guest}"
export RABBITMQ_PASSWORD="${RABBITMQ_PASSWORD:-guest}"
export RABBITMQ_VIRTUAL_HOST="${RABBITMQ_VIRTUAL_HOST:-/}"
risedev slt './e2e_test/sink/rabbitmq_sink.slt'

echo "--- e2e, http sink"
HTTP_SINK_OUTPUT=$(mktemp)
HTTP_SINK_HEADERS=$(mktemp)
python3 e2e_test/sink/http_sink_mock_server.py "$HTTP_SINK_OUTPUT" 18081 "$HTTP_SINK_HEADERS" &
HTTP_SINK_SERVER_PID=$!
# Wait for the server to be ready
for i in $(seq 1 20); do curl -sf http://localhost:18081/ && break; sleep 0.5; done

risedev slt './e2e_test/sink/http_sink.slt'

# Allow a moment for in-flight requests to complete
sleep 1
# Verify bodies reached the mock server
grep -Fx 'before update' "$HTTP_SINK_OUTPUT"
grep -Fx 'hello world' "$HTTP_SINK_OUTPUT"
grep -Fx '{"key":"value"}' "$HTTP_SINK_OUTPUT"
grep -q '"event"' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic url payload' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic url as select payload' "$HTTP_SINK_OUTPUT"
# Exactly 1 line from ignore_delete test + 2 from varchar test (NULL was skipped) + 1 from jsonb + 2 from dynamic URL tests
test "$(wc -l < "$HTTP_SINK_OUTPUT")" -eq 6
# Verify the custom header set via header.x_test = 'rw-http-sink' was sent
grep -q '"x_test": "rw-http-sink"' "$HTTP_SINK_HEADERS"
# Verify inferred default content types for varchar and jsonb payloads
grep -q '"content-type": "text/plain"' "$HTTP_SINK_HEADERS"
grep -q '"content-type": "application/json"' "$HTTP_SINK_HEADERS"

kill "$HTTP_SINK_SERVER_PID" || true
rm -f "$HTTP_SINK_OUTPUT" "$HTTP_SINK_HEADERS"

echo "--- e2e, turbopuffer sink"
TURBOPUFFER_SINK_OUTPUT=$(mktemp)
TURBOPUFFER_SINK_HEADERS=$(mktemp)
TURBOPUFFER_SINK_PATHS=$(mktemp)
python3 e2e_test/sink/http_sink_mock_server.py "$TURBOPUFFER_SINK_OUTPUT" 18082 "$TURBOPUFFER_SINK_HEADERS" "$TURBOPUFFER_SINK_PATHS" &
TURBOPUFFER_SINK_SERVER_PID=$!
# Wait for the server to be ready
for i in $(seq 1 20); do curl -sf http://localhost:18082/ && break; sleep 0.5; done

risedev slt './e2e_test/sink/turbopuffer_sink.slt'

# Allow a moment for in-flight requests to complete
sleep 1
python3 e2e_test/sink/turbopuffer_sink_check.py "$TURBOPUFFER_SINK_OUTPUT" "$TURBOPUFFER_SINK_HEADERS" "$TURBOPUFFER_SINK_PATHS"

kill "$TURBOPUFFER_SINK_SERVER_PID" || true
rm -f "$TURBOPUFFER_SINK_OUTPUT" "$TURBOPUFFER_SINK_HEADERS" "$TURBOPUFFER_SINK_PATHS"

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, ci-1cn-1fe, nexmark endless"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,await_tree::future=error" \
risedev ci-start ci-1cn-1fe
risedev slt './e2e_test/sink/nexmark_endless_mvs/*.slt'
risedev slt './e2e_test/sink/nexmark_endless_sinks/*.slt'

echo "--- Kill cluster"
risedev ci-kill
