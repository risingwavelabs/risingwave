#!/usr/bin/env bash
# Pre-create NATS JetStream stream for demo to avoid publish-ack timeout on clean starts.
# Idempotent: if stream exists, it will be kept as-is.

set -euo pipefail
set -x

SVR="nats://nats-server:4222"
STREAM="risingwave"
# Cover both json and pb demo subjects
SUBJECTS="subject1,subject2,live_stream_metrics"

# Wait for nats-server ready by using the nats CLI inside the subject1 container.
for i in {1..30}; do
  if docker compose exec subject1 nats --server "$SVR" account info >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [[ $i -eq 30 ]]; then
    echo "NATS server not ready" >&2
    exit 1
  fi
done

# If stream exists, skip creation.
if docker compose exec subject1 nats --server "$SVR" stream info "$STREAM" >/dev/null 2>&1; then
  echo "JetStream stream '$STREAM' already exists; skipping creation"
  exit 0
fi

# Create stream non-interactively; tune limits generous to avoid early backpressure in CI.
docker compose exec subject1 nats --server "$SVR" stream add "$STREAM" \
  --subjects="$SUBJECTS" \
  --storage=file \
  --retention=limits \
  --replicas=1 \
  --discard=old \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-msg-size=0 \
  --dupe-window=120s \
  --ack \
  --no-allow-rollup \
  --yes
