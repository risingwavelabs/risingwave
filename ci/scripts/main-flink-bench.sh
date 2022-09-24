#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- create a pem file to store key"
aws secretsmanager get-secret-value --secret-id "flink-bench-pem" --query "SecretString" --output text > test.pem
chmod 400 test.pem

echo "--- start the flink bench instance to run the benchmark"
aws ec2 start-instances --instance-ids i-029fdf626052dcdaf

#echo "--- do ssh and run the flink benchmark steps"
##scp -o "StrictHostKeyChecking no" -i test.pem ci/scripts/flink-bench.sh ubuntu@52.220.89.140:/home/ubuntu
##ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140 "chmod 756 flink-bench.sh"
#
#echo "--- queries to be run: $1"
##ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140 'bash -s' < ci/scripts/flink-bench.sh $1
##ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140 './flink-bench.sh $1'

echo "--- set the FLINK_HOME value"
export FLINK_HOME=/home/ubuntu/flink

echo "--- starts both zookeeper and kafka"
./start_kafka.sh
sleep 5

if jps | grep 'QuorumPeerMain'; then
  printf "zookeeper started\n"
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
sleep 5
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
sleep 5
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
./run_kafka_source.sh "${1:-q0,q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22}"
printf "completed the benchmark for all the queries\n"

echo "---- run datagen source provided by flink to generate in-memory data directly"
./run_datagen_source.sh "${1:-q0,q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22}"

echo "---- restart the flink for the graceful start of next benchmark"
./restart-flink.sh

echo "--- stop the flink bench instance"
aws ec2 stop-instances --instance-ids i-029fdf626052dcdaf
printf "stopped the flink bench instance\n"

