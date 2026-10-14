# Pulsar DNS-failure recovery

`dns_failure_recovery.py` is a manual integration test for recovery when the
Pulsar SDK terminates a consumer after a DNS lookup failure. It needs Python 3
and Docker with enough memory to run Pulsar and a single-node RisingWave instance.
It creates a private Docker network and three uniquely named containers, and
removes them on success or failure. It does not change host DNS or existing
containers.

Run against a candidate RisingWave image:

```sh
python3 e2e_test/source_legacy/pulsar/dns_failure_recovery.py \
  --risingwave-image YOUR_CANDIDATE_IMAGE
```

To test a locally built **Linux** binary, provide a runtime image with compatible
libraries and a binary matching the image's architecture:

```sh
python3 e2e_test/source_legacy/pulsar/dns_failure_recovery.py \
  --risingwave-image YOUR_BUILD_ENV_IMAGE \
  --risingwave-binary /path/to/linux/target/debug/risingwave
```

The test uses `apachepulsar/pulsar:4.0.4` and `postgres:16-alpine` by default;
`--pulsar-image` and `--client-image` override these images. The client container
supplies `psql`, `nslookup`, and `wget`, so no host database clients are needed.
Logs are saved under `.context/pulsar-dns-recovery`; use `--output-dir` to retain
multiple runs separately. `--timeout` controls each eventual assertion (default:
240 seconds, including the SDK's keepalive and connection timeouts).

The test:

1. Produces and ingests three rows, recording actor IDs and recovery counters.
2. Disconnects the broker from the network, removing its advertised hostname from
   Docker DNS, and verifies an actual NXDOMAIN response.
3. Waits for the SDK to encounter a DNS lookup error and for RisingWave to report
   unexpected consumer termination, keeping the failure active until then.
4. Produces more rows through the broker's loopback while DNS is unavailable.
5. Reconnects the broker and checks that all rows arrive exactly once, followed
   by another batch to verify continued ingestion.
6. Checks that actor IDs and cluster/database recovery counters are unchanged.
   Cluster recovery is also disabled in the test configuration, and the test
   never issues `RECOVER`.

An unfixed build fails waiting for consumer termination to reach the reader,
with the underlying DNS error retained in the RisingWave log. This test is
manual because it controls Docker network connectivity; the ordinary Pulsar SLT
CI lane does not expose the Docker daemon to its test container. The PR's
`ci/run-e2e-pulsar-source-tests` label runs the existing Pulsar source suite.
