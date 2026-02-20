#!/bin/bash

set -euo pipefail

# Wait for NATS to be ready
echo "Waiting for NATS to be ready..."
max_attempts=30
for i in $(seq 1 $max_attempts); do
  if docker compose exec -T subject1 /opt/bitnami/natscli/bin/nats stream ls -s nats://nats-server:4222 > /dev/null 2>&1; then
    echo "NATS is ready"
    break
  fi
  if [ "$i" -eq "$max_attempts" ]; then
    echo "NATS did not become ready in time"
    exit 1
  fi
  echo "Waiting for NATS... ($i/$max_attempts)"
  sleep 2
done

# Create JetStream streams for NATS sink subjects.
# The NATS sink uses JetStream context to publish, which requires a JetStream
# stream configured for the subject in order to receive publish acks.
for stream in subject1 subject2; do
  echo "Creating JetStream stream: $stream"
  docker compose exec -T subject1 /opt/bitnami/natscli/bin/nats request \
    "\$JS.API.STREAM.CREATE.${stream}" \
    "{\"name\":\"${stream}\",\"subjects\":[\"${stream}\"],\"storage\":\"memory\",\"retention\":\"limits\",\"max_msgs\":-1,\"max_bytes\":-1,\"max_age\":0,\"replicas\":1}" \
    -s nats://nats-server:4222
done

echo "NATS streams created successfully"
