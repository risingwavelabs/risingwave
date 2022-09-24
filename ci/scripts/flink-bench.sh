echo "--- set the FLINK_HOME value"
export FLINK_HOME=/home/ubuntu/flink

echo "--- starts both zookeeper and kafka"
./start_kafka.sh

if jps | grep 'QuorumPeerMain'; then
  echo "zookeeper started\n"
else
  printf "zookeeper did not start\n"
  exit 1
fi

if jps | grep 'Kafka'; then
  printf "kafka started\n"
else
  printf "kafka did not start\n"
  exit 1
fi

echo "--- start Node Exporter, Pushgateway, Prometheus and Grafana"
./start_four_monitoring_components.sh
if ./show_four_monitoring_components.sh | grep 'node_exporter'; then
  printf "node_exporter has started successfully\n"
else
  printf "node_exporter did not start\n"
  exit 1
fi

if ./show_four_monitoring_components.sh | grep 'grafana-server'; then
  printf "grafana-server has started successfully\n"
else
  printf "grafana-server did not start\n"
  exit 1
fi

if ./show_four_monitoring_components.sh | grep 'pushgateway'; then
  printf "pushgateway has started successfully\n"
else
  printf "pushgateway did not start\n"
  exit 1
fi

if ./show_four_monitoring_components.sh | grep 'prometheus'; then
  printf "prometheus has started successfully\n"
else
  printf "prometheus did not start\n"
  exit 1
fi

echo "--- starts both the Flink cluster and Nexmark monitoring service"
./restart-flink.sh
if jps | grep -e "CpuMetricSender" -e "TaskManagerRunner" > /dev/null; then
  printf "flink cluster and nexmark started."
  printf "TaskManagerRunner represents the Flink and CpuMetricSender represents Nexmark monitoring service\n"
else
  printf "flink cluster and nexmark did not start\n"
  exit 1
fi

echo "--- runs a Flink job using Datagen"
./run_insert_kafka.sh
printf "flink job has finished\n"

echo "--- check the number of records in the Kafka topic"
./show_kafka_topic_records.sh "topic"

echo "--- run the benchmark"
./run_kafka_source.sh
printf "completed the benchmark for all the queries\n"

echo "---- run Datagen source provided by Flink to generate in-memory data directly"
./run_datagen_source.sh

echo "---- restart the flink for the graceful start of next benchmark"
./restart-flink.sh