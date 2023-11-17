#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-append-only --create > /dev/null 2>&1
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert --create > /dev/null 2>&1
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert-schema --create > /dev/null 2>&1
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-debezium --create > /dev/null 2>&1

sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/create_sink.slt'
sleep 2

# test append-only kafka sink
echo "testing append-only kafka sink"
diff ./e2e_test/sink/kafka/append_only1.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-append-only --from-beginning --max-messages 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink is not as expected."
  exit 1
fi

# test upsert kafka sink
echo "testing upsert kafka sink"
diff ./e2e_test/sink/kafka/upsert1.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert --from-beginning --property print.key=true --max-messages 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink is not as expected."
  exit 1
fi

# test upsert kafka sink with schema
echo "testing upsert kafka sink with schema"
diff ./e2e_test/sink/kafka/upsert_schema1.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert-schema --from-beginning --property print.key=true --max-messages 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink with schema is not as expected."
  exit 1
fi

# test debezium kafka sink
echo "testing debezium kafka sink"
(./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-debezium --property print.key=true --from-beginning --max-messages 10 | sort) > ./e2e_test/sink/kafka/debezium1.tmp.result 2> /dev/null
python3 e2e_test/sink/kafka/debezium.py e2e_test/sink/kafka/debezium1.result e2e_test/sink/kafka/debezium1.tmp.result
if [ $? -ne 0 ]; then
  echo "The output for debezium sink is not as expected."
  rm e2e_test/sink/kafka/debezium1.tmp.result
  exit 1
else
  rm e2e_test/sink/kafka/debezium1.tmp.result
fi

# update sink data
echo "updating sink data"
psql -h localhost -p 4566 -d dev -U root -c "update t_kafka set v_varchar = '', v_smallint = 0, v_integer = 0, v_bigint = 0, v_float = 0.0, v_double = 0.0, v_timestamp = '1970-01-01 00:00:00.0' where id = 1;" > /dev/null

# test append-only kafka sink after update
echo "testing append-only kafka sink after updating data"
diff ./e2e_test/sink/kafka/append_only2.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-append-only --from-beginning --max-messages 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink after update is not as expected."
  exit 1
fi

# test upsert kafka sink after update
echo "testing upsert kafka sink after updating data"
diff ./e2e_test/sink/kafka/upsert2.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert --from-beginning --property print.key=true --max-messages 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink after update is not as expected."
  exit 1
fi

# test upsert kafka sink with schema after update
echo "testing upsert kafka sink with schema after updating data"
diff ./e2e_test/sink/kafka/upsert_schema2.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert-schema --from-beginning --property print.key=true --max-messages 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink with schema is not as expected."
  exit 1
fi

# test debezium kafka sink after update
echo "testing debezium kafka sink after updating data"
(./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-debezium --property print.key=true --from-beginning --max-messages 11  | sort) > ./e2e_test/sink/kafka/debezium2.tmp.result 2> /dev/null
python3 e2e_test/sink/kafka/debezium.py e2e_test/sink/kafka/debezium2.result e2e_test/sink/kafka/debezium2.tmp.result
if [ $? -ne 0 ]; then
  echo "The output for debezium sink after update is not as expected."
  rm e2e_test/sink/kafka/debezium2.tmp.result
  exit 1
else
  rm e2e_test/sink/kafka/debezium2.tmp.result
fi

# delete sink data
echo "deleting sink data"
psql -h localhost -p 4566 -d dev -U root -c "delete from t_kafka where id = 1;" > /dev/null

# test upsert kafka sink after delete
echo "testing upsert kafka sink after deleting data"
diff ./e2e_test/sink/kafka/upsert3.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert --from-beginning --property print.key=true --max-messages 12 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink after update is not as expected."
  exit 1
fi

# test upsert kafka sink with schema after delete
echo "testing upsert kafka sink with schema after deleting data"
diff ./e2e_test/sink/kafka/upsert_schema3.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert-schema --from-beginning --property print.key=true --max-messages 12 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink with schema is not as expected."
  exit 1
fi

# test debezium kafka sink after delete
echo "testing debezium kafka sink after deleting data"
(./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-debezium --property print.key=true --from-beginning --max-messages 13 | sort) > ./e2e_test/sink/kafka/debezium3.tmp.result 2> /dev/null
python3 e2e_test/sink/kafka/debezium.py e2e_test/sink/kafka/debezium3.result e2e_test/sink/kafka/debezium3.tmp.result
if [ $? -ne 0 ]; then
  echo "The output for debezium sink after delete is not as expected."
  rm e2e_test/sink/kafka/debezium3.tmp.result
  exit 1
else
  rm e2e_test/sink/kafka/debezium3.tmp.result
fi

sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/drop_sink.slt'
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-append-only --delete > /dev/null 2>&1
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert --delete > /dev/null 2>&1
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-debezium --delete > /dev/null 2>&1

# test different encoding
echo "testing protobuf"
cp src/connector/src/test_data/proto_recursive/recursive.pb ./proto-recursive
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-append-only-protobuf --create > /dev/null 2>&1
sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/protobuf.slt'
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-append-only-protobuf --delete > /dev/null 2>&1

echo "testing avro"
python3 -m pip install requests confluent-kafka
python3 e2e_test/sink/kafka/register_schema.py 'http://message_queue:8081' 'test-rw-sink-upsert-avro-value' src/connector/src/test_data/all-types.avsc
python3 e2e_test/sink/kafka/register_schema.py 'http://message_queue:8081' 'test-rw-sink-upsert-avro-key' src/connector/src/test_data/all-types.avsc 'string_field,int32_field'
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert-avro --create > /dev/null 2>&1
sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/avro.slt'
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server message_queue:29092 --topic test-rw-sink-upsert-avro --delete > /dev/null 2>&1
