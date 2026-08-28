#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/functions.sh"

keycloak_uri="${KEYCLOAK_URI:-http://${KEYCLOAK_HOST:-keycloak_oidc}:8080}"
realm="${REALM:-demo}"

wait_for_url "${keycloak_uri}/realms/${realm}" "Waiting for realm '${realm}' to be available"

cat > /tmp/strimzi.properties <<EOF
process.roles=${KAFKA_PROCESS_ROLES:-broker,controller}
node.id=${KAFKA_NODE_ID:-1}
log.dirs=${KAFKA_LOG_DIRS:-/tmp/kraft-combined-logs}

controller.quorum.voters=${KAFKA_CONTROLLER_QUORUM_VOTERS:-1@message_queue_oidc:9091}
controller.listener.names=${KAFKA_CONTROLLER_LISTENER_NAMES:-CONTROLLER}
listeners=${KAFKA_LISTENERS:-CONTROLLER://message_queue_oidc:9091,CLIENT://0.0.0.0:9092}
advertised.listeners=${KAFKA_ADVERTISED_LISTENERS:-CLIENT://message_queue_oidc:9092}
listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:-CONTROLLER:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT}

num.network.threads=${KAFKA_NUM_NETWORK_THREADS:-3}
num.io.threads=${KAFKA_NUM_IO_THREADS:-8}
socket.send.buffer.bytes=${KAFKA_SOCKET_SEND_BUFFER_BYTES:-102400}
socket.receive.buffer.bytes=${KAFKA_SOCKET_RECEIVE_BUFFER_BYTES:-102400}
socket.request.max.bytes=${KAFKA_SOCKET_REQUEST_MAX_BYTES:-104857600}
num.partitions=${KAFKA_NUM_PARTITIONS:-1}
num.recovery.threads.per.data.dir=${KAFKA_NUM_RECOVERY_THREADS_PER_DATA_DIR:-1}
offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-1}
transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-1}
transaction.state.log.min.isr=${KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:-1}
log.retention.hours=${KAFKA_LOG_RETENTION_HOURS:-168}
log.segment.bytes=${KAFKA_LOG_SEGMENT_BYTES:-1073741824}
log.retention.check.interval.ms=${KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS:-300000}

sasl.enabled.mechanisms=${KAFKA_SASL_ENABLED_MECHANISMS:-OAUTHBEARER}
inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME:-CLIENT}
sasl.mechanism.inter.broker.protocol=${KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL:-OAUTHBEARER}
sasl.mechanism.controller.protocol=${KAFKA_SASL_MECHANISM_CONTROLLER_PROTOCOL:-SCRAM-SHA-512}
listener.name.controller.sasl.enabled.mechanisms=${KAFKA_LISTENER_NAME_CONTROLLER_SASL_ENABLED_MECHANISMS:-SCRAM-SHA-512}
listener.name.controller.scram-sha-512.sasl.jaas.config=${KAFKA_LISTENER_NAME_CONTROLLER_SCRAM_SHA_512_SASL_JAAS_CONFIG:-org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin-secret\" ;}
listener.name.client.oauthbearer.sasl.jaas.config=${KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_JAAS_CONFIG:-org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;}
listener.name.client.oauthbearer.sasl.login.callback.handler.class=${KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_LOGIN_CALLBACK_HANDLER_CLASS:-io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler}
listener.name.client.oauthbearer.sasl.server.callback.handler.class=${KAFKA_LISTENER_NAME_CLIENT_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS:-io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler}
super.users=${KAFKA_SUPER_USERS:-User:service-account-kafka-broker}
EOF

echo "Generated Kafka OIDC server properties:"
cat /tmp/strimzi.properties

kafka_cluster_id="$("/opt/kafka/bin/kafka-storage.sh" random-uuid)"
"/opt/kafka/bin/kafka-storage.sh" format -t "${kafka_cluster_id}" -c /tmp/strimzi.properties \
    --add-scram 'SCRAM-SHA-512=[name=admin,password=admin-secret]'

exec /opt/kafka/bin/kafka-server-start.sh /tmp/strimzi.properties
