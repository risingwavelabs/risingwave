#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- create a pem file to store key"
aws secretsmanager get-secret-value --secret-id "flink-bench-pem" --query "SecretString" --output text > test.pem
chmod 400 test.pem

echo "--- start the flink bench instance to run the benchmark"
aws ec2 start-instances --instance-ids i-029fdf626052dcdaf

echo "--- do ssh to the flink benchmark machine"
ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140
printf "logged in to the flink benchmark setup\n"

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
if ./show_four_monitoring_components.sh | grep 'node_exporter' | grep 'grafana-server' | grep 'pushgateway' | grep 'prometheus'; then
  printf "all services have started successfully\n"
else
  printf "above services did not start\n"
  exit 1
fi

echo "--- starts both the Flink cluster and Nexmark monitoring service"
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

echo "--- stop the flink bench instance"
aws ec2 stop-instances --instance-ids i-029fdf626052dcdaf
printf "stopped the flink bench instance\n"

