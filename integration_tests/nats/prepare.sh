#!/usr/bin/env bash
# Pre-create JetStream streams needed by the NATS sink before running the demo.
# The NATS sink uses JetStream context to publish, which requires a stream
# configured for the subject in order to receive publish acks. Without a stream,
# the ack never arrives and async-nats times out (~5s), causing actors to exit
# with "Nats sink error: timed out: didn't receive ack in time".
#
# Idempotent: if a stream already exists it is skipped.

set -euo pipefail

SVR="nats://nats-server:4222"
NATS="/opt/bitnami/natscli/bin/nats"

# Wait for nats-server to be ready.
for i in $(seq 1 30); do
  if docker compose exec -T subject1 "$NATS" --server "$SVR" account info > /dev/null 2>&1; then
    break
  fi
  sleep 2
  if [ "$i" -eq 30 ]; then
    echo "NATS server not ready" >&2
    exit 1
  fi
done

# Create separate JetStream streams for each sink subject.
# subject1 and subject2 are used by the JSON and protobuf sink tests respectively.
for stream in subject1 subject2; do
  if docker compose exec -T subject1 "$NATS" --server "$SVR" stream info "$stream" > /dev/null 2>&1; then
    echo "JetStream stream '$stream' already exists; skipping"
    continue
  fi
  echo "Creating JetStream stream: $stream"
  docker compose exec -T subject1 "$NATS" --server "$SVR" stream add "$stream" \
    --subjects="$stream" \
    --storage=memory \
    --retention=limits \
    --replicas=1 \
    --discard=old \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-msg-size=-1 \
    --dupe-window=120s \
    --ack \
    --defaults
done

echo "NATS streams created successfully"
