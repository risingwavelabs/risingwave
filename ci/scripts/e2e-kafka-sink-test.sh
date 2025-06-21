#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export RPK_BROKERS="message_queue:29092"

rpk topic create test-rw-sink-append-only
rpk topic create test-rw-sink-upsert
rpk topic create test-rw-sink-upsert-schema
rpk topic create test-rw-sink-debezium
rpk topic create test-rw-sink-without-snapshot
rpk topic create test-rw-sink-text-key-id
rpk topic create test-rw-sink-bytes-key-id

sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/create_sink.slt'
sleep 2

# test append-only kafka sink
echo "testing append-only kafka sink"
diff -b ./e2e_test/sink/kafka/append_only1.result \
<((rpk topic consume test-rw-sink-append-only --offset start --format '%v\n' --num 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink is not as expected."
  exit 1
fi

# test upsert kafka sink
echo "testing upsert kafka sink"
diff -b ./e2e_test/sink/kafka/upsert1.result \
<((rpk topic consume test-rw-sink-upsert --offset start --format '%k\t%v\n' --num 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink is not as expected."
  exit 1
fi

# test upsert kafka sink with schema
echo "testing upsert kafka sink with schema"
diff -b ./e2e_test/sink/kafka/upsert_schema1.result \
<((rpk topic consume test-rw-sink-upsert-schema --offset start --format '%k\t%v\n' --num 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink with schema is not as expected."
  exit 1
fi

# test debezium kafka sink
echo "testing debezium kafka sink"
(rpk topic consume test-rw-sink-debezium --offset start --format '%k\t%v\n'  --num 10 | sort) > ./e2e_test/sink/kafka/debezium1.tmp.result 2> /dev/null
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
diff -b ./e2e_test/sink/kafka/append_only2.result \
<((rpk topic consume test-rw-sink-append-only --offset start --format '%v\n' --num 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink after update is not as expected."
  exit 1
fi

# test upsert kafka sink after update
echo "testing upsert kafka sink after updating data"
diff -b ./e2e_test/sink/kafka/upsert2.result \
<((rpk topic consume test-rw-sink-upsert --offset start --format '%k\t%v\n' --num 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink after update is not as expected."
  exit 1
fi

# test upsert kafka sink with schema after update
echo "testing upsert kafka sink with schema after updating data"
diff -b ./e2e_test/sink/kafka/upsert_schema2.result \
<((rpk topic consume test-rw-sink-upsert-schema --offset start --format '%k\t%v\n' --num 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink with schema is not as expected."
  exit 1
fi

# test debezium kafka sink after update
echo "testing debezium kafka sink after updating data"
(rpk topic consume test-rw-sink-debezium --offset start --format '%k\t%v\n' --num 11 | sort) > ./e2e_test/sink/kafka/debezium2.tmp.result 2> /dev/null
python3 e2e_test/sink/kafka/debezium.py e2e_test/sink/kafka/debezium2.result e2e_test/sink/kafka/debezium2.tmp.result
if [ $? -ne 0 ]; then
  echo "The output for debezium sink after update is not as expected."
  rm e2e_test/sink/kafka/debezium2.tmp.result
  exit 1
else
  rm e2e_test/sink/kafka/debezium2.tmp.result
fi

# test without-snapshot kafka sink
echo "testing without-snapshot kafka sink"
diff -b ./e2e_test/sink/kafka/without_snapshot.result \
<((rpk topic consume test-rw-sink-without-snapshot --offset start --format '%v\n' --num 3 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink is not as expected."
  exit 1
fi

# delete sink data
echo "deleting sink data"
psql -h localhost -p 4566 -d dev -U root -c "delete from t_kafka where id = 1;" > /dev/null

# test upsert kafka sink after delete
echo "testing upsert kafka sink after deleting data"
diff -b ./e2e_test/sink/kafka/upsert3.result \
<((rpk topic consume test-rw-sink-upsert --offset start --format '%k\t%v\n' --num 12 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink after update is not as expected."
  exit 1
fi

# test upsert kafka sink with schema after delete
echo "testing upsert kafka sink with schema after deleting data"
diff -b ./e2e_test/sink/kafka/upsert_schema3.result \
<((rpk topic consume test-rw-sink-upsert-schema --offset start --format '%k\t%v\n' --num 12 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink with schema is not as expected."
  exit 1
fi

# test debezium kafka sink after delete
echo "testing debezium kafka sink after deleting data"
(rpk topic consume test-rw-sink-debezium --offset start --format '%k\t%v\n' --num 13 | sort) > ./e2e_test/sink/kafka/debezium3.tmp.result 2> /dev/null
python3 e2e_test/sink/kafka/debezium.py e2e_test/sink/kafka/debezium3.result e2e_test/sink/kafka/debezium3.tmp.result
if [ $? -ne 0 ]; then
  echo "The output for debezium sink after delete is not as expected."
  rm e2e_test/sink/kafka/debezium3.tmp.result
  exit 1
else
  rm e2e_test/sink/kafka/debezium3.tmp.result
fi

sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/drop_sink.slt'
rpk topic delete test-rw-sink-append-only
rpk topic delete test-rw-sink-upsert
rpk topic delete test-rw-sink-debezium

# test different encoding
echo "preparing confluent schema registry"
python3 -m pip install --break-system-packages -r e2e_test/requirements.txt

echo "testing protobuf"
risedev slt 'e2e_test/sink/kafka/protobuf.slt'

echo "testing avro"
risedev slt 'e2e_test/sink/kafka/avro.slt'

echo "testing avro-decimal"
risedev slt 'e2e_test/sink/kafka/avro-decimal.slt'
