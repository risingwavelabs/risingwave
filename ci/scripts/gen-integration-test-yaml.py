#!/usr/bin/env python3

import subprocess
from typing import Dict, Optional

CASES_MAP = {
    "ad-click": ["json"],
    "ad-ctr": ["json"],
    "cdn-metrics": ["json"],
    "clickstream": ["json"],
    "livestream": ["json", "protobuf"],
    "prometheus": ["json"],
    "mysql-cdc": ["json"],
    "postgres-cdc": ["json"],
    "mongodb-cdc": ["json"],
    "mysql-sink": ["json"],
    "postgres-sink": ["json"],
    "iceberg-cdc": ["json"],
    "iceberg-sink": ["none"],
    "iceberg-source": ["none"],
    "twitter": ["json", "protobuf"],
    "twitter-pulsar": ["json"],
    "debezium-mysql": ["json"],
    "debezium-postgres": ["json"],
    "debezium-sqlserver": ["json"],
    "tidb-cdc-sink": ["json"],
    "citus-cdc": ["json"],
    "kinesis-s3-source": ["json"],
    "clickhouse-sink": ["json"],
    "cockroach-sink": ["json"],
    "kafka-cdc-sink": ["json"],
    "cassandra-and-scylladb-sink": ["json"],
    "elasticsearch-sink": ["json"],
    "redis-sink": ["json"],
    "big-query-sink": ["json"],
    "mindsdb": ["json"],
    "vector": ["json"],
    "nats": ["json", "protobuf"],
    "mqtt": ["json"],
    "doris-sink": ["json"],
    "starrocks-sink": ["json"],
    "deltalake-sink": ["json"],
    "pinot-sink": ["json"],
    "presto-trino": ["json"],
    "client-library": ["none"],
    "kafka-cdc": ["json"],
    "pubsub": ["json"],
    "dynamodb": ["json"],
}

def gen_step(
    test_case: str,
    test_format: str,
    *,
    step_key: Optional[str] = None,
    extra_env: Optional[Dict[str, str]] = None,
) -> str:
    key = step_key or f"{test_case}-{test_format}"
    env_block = ""
    if extra_env:
        env_lines = "\n".join([f"     {k}: {v}" for k, v in extra_env.items()])
        env_block = f"\n   env:\n{env_lines}"
    return f"""
 - label: Run Demos {key}
   key: {key}
   command: ci/scripts/integration-tests.sh -c {test_case} -f {test_format}
   timeout_in_minutes: 30
   retry: *auto-retry
   concurrency: 10
   concurrency_group: 'integration-test/run'
   plugins:
     - seek-oss/aws-sm#v2.3.2:
         env:
           GHCR_USERNAME: ghcr-username
           GHCR_TOKEN: ghcr-token
           RW_LICENSE_KEY: rw-license-key
     - ./ci/plugins/docker-compose-logs{env_block}
"""


def gen_pipeline_steps():
    pipeline_steps = ""
    for test_case, test_formats in CASES_MAP.items():
        for test_format in test_formats:
            pipeline_steps += gen_step(test_case, test_format)
            if test_case == "postgres-cdc" and test_format == "json":
                pipeline_steps += gen_step(
                    test_case,
                    test_format,
                    step_key="postgres-cdc-pg18-json",
                    extra_env={"POSTGRES_CDC_IMAGE": "postgres:18-alpine"},
                )
    return pipeline_steps

def format_pipeline_yaml_cmd(pipeline_steps):
    pipeline_yaml=f"""
cat <<- YAML | buildkite-agent pipeline upload
auto-retry: &auto-retry
  automatic:
    # Agent terminated because the AWS EC2 spot instance killed by AWS.
    - signal_reason: agent_stop
      limit: 3
    - exit_status: -1
      signal_reason: none
      limit: 3

steps: {pipeline_steps}
YAML
"""
    return pipeline_yaml

def main():
    pipeline_steps = gen_pipeline_steps()
    cmd = format_pipeline_yaml_cmd(pipeline_steps)
    print(cmd)
    subprocess.run(cmd, shell=True)
    print("upload pipeline yaml")

if __name__ == "__main__":
    main()
