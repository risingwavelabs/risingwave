#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- create a pem file to store key"
aws secretsmanager get-secret-value --secret-id "flink-bench-pem" --query "SecretString" --output text > test.pem
chmod 400 test.pem

echo "--- start the flink bench instance to run the benchmark"
aws ec2 start-instances --instance-ids i-029fdf626052dcdaf

echo "--- do ssh and run the flink benchmark steps"
scp -o "StrictHostKeyChecking no" -i test.pem ci/scripts/flink-bench.sh ubuntu@52.220.89.140:/home/ubuntu
ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140 "chmod 756 flink-bench.sh"

echo "--- queries to be run: $1"
#ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140 'bash -s' < ci/scripts/flink-bench.sh $1
ssh -o "StrictHostKeyChecking no" -i test.pem ubuntu@52.220.89.140 'bash flink-bench.sh $1'

echo "--- stop the flink bench instance"
aws ec2 stop-instances --instance-ids i-029fdf626052dcdaf
printf "stopped the flink bench instance\n"

