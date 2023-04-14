#!/bin/bash

sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/create_sink.slt'
sleep 2

echo "testing append-only and upsert kafka sink"

# test upsert kafka sink
diff ./e2e_test/sink/kafka/upsert1.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:29092 --topic test-rw-sink-upsert --from-beginning --property print.key=true --max-messages 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink is not as expected."
  exit 1
fi

# test append-only kafka sink
diff ./e2e_test/sink/kafka/append_only1.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:29092 --topic test-rw-sink-append-only --from-beginning --max-messages 10 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink is not as expected."
  exit 1
fi

# update sink data
psql -h localhost -p 4566 -d dev -U root -c "update t_kafka set v_varchar = '', v_smallint = 0, v_integer = 0, v_bigint = 0, v_float = 0.0, v_double = 0.0, v_timestamp = '1970-01-01 00:00:00.0' where id = 1;" > /dev/null

echo "testing append-only and upsert kafka sink after updating data"

# test upsert kafka sink after update
diff ./e2e_test/sink/kafka/upsert2.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:29092 --topic test-rw-sink-upsert --from-beginning --property print.key=true --max-messages 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for upsert sink after update is not as expected."
  exit 1
fi

# test append-only kafka sink after update
diff ./e2e_test/sink/kafka/append_only2.result \
<((./.risingwave/bin/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:29092 --topic test-rw-sink-append-only --from-beginning --max-messages 11 | sort) 2> /dev/null)
if [ $? -ne 0 ]; then
  echo "The output for append-only sink after update is not as expected."
  exit 1
fi

sqllogictest -p 4566 -d dev 'e2e_test/sink/kafka/drop_sink.slt'
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic test-rw-sink-upsert --delete > /dev/null 2>&1
./.risingwave/bin/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:29092 --topic test-rw-sink-append-only --delete > /dev/null 2>&1
