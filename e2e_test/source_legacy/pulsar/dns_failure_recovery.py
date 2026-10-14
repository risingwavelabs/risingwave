#!/usr/bin/env python3
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Manual Docker integration test for Pulsar recovery after live DNS failure.

See README.md for requirements and usage. All injected failures are confined to
containers created by this script; host DNS and existing containers are untouched.
"""

import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid


BROKER_HOST = "pulsar-dns.test"
TOPIC = "persistent://public/default/dns-recovery"
TERMINATION = "unexpected Pulsar consumer termination"


def docker(*args, check=True, timeout=60):
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout
    )
    if check and result.returncode:
        raise RuntimeError(f"docker {args}:\n{result.stdout}{result.stderr}")
    return result


def wait_for(description, check, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            print(description, flush=True)
            return
        time.sleep(2)
    raise TimeoutError(f"Timed out after {timeout}s: {description}")


class Test:
    def __init__(self, args):
        self.args = args
        self.prefix = f"rw-pulsar-dns-{uuid.uuid4().hex[:10]}"
        self.network = self.prefix
        self.broker = f"{self.prefix}-broker"
        self.rw = f"{self.prefix}-rw"
        self.client = f"{self.prefix}-client"
        self.containers = []
        self.output = args.output_dir.resolve()
        self.output.mkdir(parents=True, exist_ok=True)

    def start(self, name, *args):
        self.containers.append(name)
        docker("run", "-d", "--name", name, "--network", self.network, *args)

    def sql(self, sql, check=True):
        return docker(
            "exec", "-e", "PGCONNECT_TIMEOUT=5", "-e", "PGOPTIONS=-c statement_timeout=10000",
            self.client, "psql", "-X", "-h", "risingwave", "-p", "4566",
            "-U", "root", "-d", "dev", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql,
            check=check,
        )

    def logs(self, since):
        result = docker("logs", "--since", since, self.rw)
        return result.stdout + result.stderr

    def produce(self, ids):
        messages = ",".join(json.dumps({"id": i}) for i in ids)
        docker(
            "exec", self.broker, "bin/pulsar-client", "--url", "pulsar://localhost:6650",
            "produce", TOPIC, "--messages", messages, "--disable-batching",
        )

    def expect_rows(self, ids):
        expected = "\n".join(str(i) for i in ids)
        wait_for(
            f"Ingested exactly {list(ids)}",
            lambda: self.sql("SELECT id FROM dns_recovery ORDER BY id").stdout.strip() == expected,
            self.args.timeout,
        )

    def recovery_counts(self):
        metrics = docker(
            "exec", self.client, "wget", "-qO-", "http://risingwave:1250/metrics"
        ).stdout
        counts = sorted(
            line for line in metrics.splitlines()
            if line.startswith(("recovery_latency_count", "recovery_failure_cnt"))
        )
        if not counts:
            raise AssertionError("Recovery metrics missing; cannot rule out actor recovery")
        return counts

    def dns_unavailable(self):
        result = docker("exec", self.client, "nslookup", BROKER_HOST, check=False)
        if result.returncode and "NXDOMAIN" in result.stdout:
            (self.output / "dns-failure.txt").write_text(result.stdout + result.stderr)
            return True
        return False

    def run(self):
        docker("network", "create", self.network)
        try:
            self.start(
                self.broker, "--network-alias", BROKER_HOST, "--hostname", BROKER_HOST,
                "--add-host", f"{BROKER_HOST}:127.0.0.1",
                "-e", "PULSAR_MEM=-Xms256m -Xmx512m -XX:MaxDirectMemorySize=256m",
                self.args.pulsar_image, "bin/pulsar", "standalone", "--no-functions-worker",
                "--advertised-address", BROKER_HOST,
            )
            self.start(self.client, "--entrypoint", "sleep", self.args.client_image, "infinity")
            wait_for(
                "Pulsar broker ready",
                lambda: docker(
                    "exec", self.broker, "curl", "-fsS", "--max-time", "5",
                    "http://localhost:8080/admin/v2/brokers/ready", check=False,
                ).returncode == 0,
                self.args.timeout,
            )
            # Keep messages produced during the outage even while the reader's
            # non-durable subscription is absent.
            docker(
                "exec", self.broker, "bin/pulsar-admin", "namespaces", "set-retention",
                "public/default", "--size", "10M", "--time", "10m",
            )
            config = self.output / "risingwave.toml"
            config.write_text(
                "[meta]\ndisable_recovery = true\n"
                "[system]\nbarrier_interval_ms = 200\ncheckpoint_frequency = 5\n"
            )
            rw_args = [
                "--network-alias", "risingwave",
                "-v", f"{config}:/test/risingwave.toml:ro",
                "-e", "ENABLE_TELEMETRY=false",
                "-e", "RUST_LOG=info,pulsar=debug",
            ]
            binary = "/risingwave/bin/risingwave"
            if self.args.risingwave_binary:
                binary = "/test/risingwave"
                rw_args += ["-v", f"{self.args.risingwave_binary.resolve()}:{binary}:ro"]
            self.start(
                self.rw, *rw_args, "--entrypoint", binary, self.args.risingwave_image,
                "--in-memory", "--listen-addr", "0.0.0.0:4566", "--parallelism", "1",
                "--total-memory-bytes", "2147483648",
                "--prometheus-listener-addr", "0.0.0.0:1250",
                "--config-path", "/test/risingwave.toml",
            )
            wait_for(
                "RisingWave ready",
                lambda: self.sql("SELECT 1", check=False).returncode == 0,
                self.args.timeout,
            )
            self.produce(range(1, 4))
            self.sql(f"""
                CREATE TABLE dns_recovery (id INT) WITH (
                    connector = 'pulsar',
                    service.url = 'pulsar://{BROKER_HOST}:6650',
                    topic = '{TOPIC}',
                    scan.startup.mode = 'earliest'
                ) FORMAT PLAIN ENCODE JSON
            """)
            self.expect_rows(range(1, 4))
            before_recovery = self.recovery_counts()
            actors_query = "SELECT actor_id FROM rw_catalog.rw_actors ORDER BY actor_id"
            before_actors = self.sql(actors_query).stdout
            fault_start = str(time.time())

            # Docker's embedded DNS removes the broker alias when this interface
            # disappears. Its loopback remains available to the producer via exec.
            docker("network", "disconnect", self.network, self.broker)
            wait_for(
                "Broker DNS returns NXDOMAIN",
                self.dns_unavailable,
                30,
            )
            # Keep the fault active until the SDK has actually terminated, rather
            # than restoring DNS after an arbitrary sleep that might miss the bug.
            wait_for(
                "Pulsar consumer terminated after DNS failure",
                lambda: TERMINATION in self.logs(fault_start),
                self.args.timeout,
            )
            fault_logs = self.logs(fault_start)
            if "could not look up address" not in fault_logs:
                raise AssertionError("Consumer terminated without evidence of a DNS lookup error")
            self.expect_rows(range(1, 4))
            self.produce(range(4, 7))
            self.expect_rows(range(1, 4))

            docker("network", "connect", "--alias", BROKER_HOST, self.network, self.broker)
            wait_for(
                "Broker DNS restored",
                lambda: docker(
                    "exec", self.client, "nslookup", BROKER_HOST, check=False
                ).returncode == 0,
                30,
            )
            self.expect_rows(range(1, 7))
            self.produce(range(7, 10))
            self.expect_rows(range(1, 10))
            if self.recovery_counts() != before_recovery:
                raise AssertionError("Cluster/database recovery masked source-reader recovery")
            if self.sql(actors_query).stdout != before_actors:
                raise AssertionError("Actors changed during recovery")
            self.sql("DROP TABLE dns_recovery")
            print(
                "PASS: DNS failure recovered without RECOVER, actor rebuild, "
                "missing rows, or duplicates",
                flush=True,
            )
        finally:
            for name in self.containers:
                result = docker("logs", name, check=False)
                (self.output / f"{name}.log").write_text(result.stdout + result.stderr)
            for name in reversed(self.containers):
                docker("rm", "-f", "-v", name, check=False)
            docker("network", "rm", self.network, check=False)
            print(f"Logs: {self.output}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--risingwave-image", required=True,
        help="Image containing the candidate RisingWave build or its runtime libraries",
    )
    parser.add_argument(
        "--risingwave-binary", type=Path,
        help="Optional Linux binary to mount into the runtime image",
    )
    parser.add_argument("--pulsar-image", default="apachepulsar/pulsar:4.0.4")
    parser.add_argument("--client-image", default="postgres:16-alpine")
    parser.add_argument("--output-dir", type=Path, default=Path(".context/pulsar-dns-recovery"))
    parser.add_argument(
        "--timeout", type=int, default=240,
        help="Timeout per eventually consistent assertion, in seconds",
    )
    args = parser.parse_args()
    if args.risingwave_binary and not args.risingwave_binary.is_file():
        parser.error("--risingwave-binary must name an existing Linux executable")
    Test(args).run()


if __name__ == "__main__":
    main()
