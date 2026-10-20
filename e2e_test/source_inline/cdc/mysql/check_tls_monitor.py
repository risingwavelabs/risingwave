#!/usr/bin/env python3
"""Assert that each TLS source has successfully queried upstream binlogs."""

import os
import re
import subprocess
import time
import urllib.error
import urllib.request

source_ids = subprocess.check_output(
    [
        "psql", "-h", os.environ["SLT_HOST"], "-p", os.environ["SLT_PORT"],
        "-d", os.environ["SLT_DB"], "-U", "root", "-Atc",
        "SELECT id FROM rw_catalog.rw_sources WHERE name IN "
        "('s_mysql_tls_required', 's_mysql_tls_verify_ca', "
        "'s_mysql_tls_verify_full', 's_mysql_tls_alias')",
    ], text=True,
).splitlines()
assert len(source_ids) == 4, source_ids
deadline = time.monotonic() + 90
last_error = None
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(
            os.environ.get("MYSQL_TLS_META_METRICS_URL", "http://127.0.0.1:1250/metrics"), timeout=5
        ) as response:
            metrics = response.read().decode()
        for source_id in source_ids:
            for metric in ["mysql_cdc_binlog_file_seq_min", "mysql_cdc_binlog_file_seq_max"]:
                values = [
                    float(value)
                    for labels, value in re.findall(rf"^{metric}\{{([^}}]+)\}} (\S+)$", metrics, re.M)
                    if f'source_id="{source_id}"' in labels.split(",")
                ]
                assert len(values) == 1 and values[0] > 0, (source_id, metric, values)
        break
    except (AssertionError, urllib.error.URLError, TimeoutError) as error:
        last_error = error
        time.sleep(1)
else:
    raise AssertionError("TLS binlog monitoring did not succeed") from last_error
