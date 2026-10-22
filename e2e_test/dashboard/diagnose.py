# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import subprocess
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


def sql(database, query):
    return subprocess.check_output(
        [
            "psql",
            "-XAt",
            "-v", "ON_ERROR_STOP=1",
            "-h", os.environ["SLT_HOST"],
            "-p", os.environ["SLT_PORT"],
            "-U", "root",
            "-d", database,
            "-c", query,
        ],
        text=True,
    ).strip()


def report(**params):
    # Single-node CI uses the default dashboard port without a risedev endpoint.
    endpoint = os.environ.get("RISEDEV_RW_META_DASHBOARD_ADDR", "http://localhost:5691")
    url = f"{endpoint.rstrip('/')}/api/monitor/diagnose/?{urlencode(params)}"
    with urlopen(url, timeout=60) as response:
        return response.read().decode()


def main():
    databases = ("diagnose_filter_a", "diagnose_filter_b")
    ids = {}
    actors = {}
    names = {}
    for database in databases:
        schema = f"{database}_schema"
        names[database] = [
            f"{database}_{kind}"
            for kind in ("table", "mv", "index", "view", "source", "sink")
        ]
        table, mv, index, view, source, sink = names[database]
        sql(database, f"""
            CREATE SCHEMA {schema};
            CREATE TABLE {schema}.{table} (v int);
            CREATE MATERIALIZED VIEW {schema}.{mv} AS SELECT count(*) FROM {schema}.{table};
            CREATE INDEX {index} ON {schema}.{table} (v);
            CREATE VIEW {schema}.{view} AS SELECT * FROM {schema}.{table};
            CREATE SOURCE {schema}.{source} (v int) WITH (connector = 'datagen') FORMAT PLAIN ENCODE JSON;
            CREATE SINK {schema}.{sink} FROM {schema}.{mv} WITH (connector = 'blackhole');
        """)
        ids[database] = int(sql(
            database,
            f"SELECT id FROM rw_catalog.rw_databases WHERE name = '{database}'",
        ))
        actors[database] = set(sql(database, f"""
            SELECT a.actor_id FROM rw_catalog.rw_actors a
            JOIN rw_catalog.rw_fragments f ON a.fragment_id = f.fragment_id
            JOIN rw_catalog.rw_streaming_jobs j ON f.table_id = j.id
            WHERE j.database_id = {ids[database]}
        """).splitlines())
        assert actors[database], f"No actors found in {database}"
        names[database].append(schema)

    # Omitting the filter preserves the full report.
    full = report()
    for database in databases:
        assert all(name in full for name in names[database])

    for database, other in (databases, databases[::-1]):
        for trace_format in ("text", "json"):
            filtered = report(database_id=ids[database], actor_traces_format=trace_format)
            # Check the catalog independently of shared traces and metrics.
            catalog = filtered.split("ACTOR\n", 1)[0]
            assert all(name in catalog for name in names[database]), database
            assert all(name not in catalog for name in names[other]), other
            actor_table = filtered.split("ACTOR\n", 1)[1].split("top sources", 1)[0]
            assert f"{database}_table" in actor_table
            assert f"{other}_table" not in actor_table
            trace_ids = set(re.findall(r"^>> Actor (\d+)$", filtered, re.MULTILINE))
            assert trace_ids <= actors[database], trace_ids - actors[database]
            # Some cluster configurations disable await trees.
            if re.search(r"^>> Actor \d+$", full, re.MULTILINE):
                assert trace_ids, "Filtered report lost all actor traces"
            completions = filtered.split("latest barrier completions\n", 1)[1].split(
                "latest barrier collection failures\n", 1
            )[0]
            event_database_ids = set(re.findall(r'"databaseId":(\d+)', completions))
            assert event_database_ids <= {str(ids[database])}, event_database_ids
            assert "SYSTEM PARAMS" in filtered
            assert "LICENSE" in filtered

    empty = report(database_id=4294967295)
    empty_catalog = empty.split("ACTOR\n", 1)[0]
    assert all(
        name not in empty_catalog for values in names.values() for name in values
    )
    assert not re.search(r"^>> Actor \d+$", empty, re.MULTILINE)

    for invalid in ("invalid", "-1", "4294967296"):
        try:
            report(database_id=invalid)
        except HTTPError as error:
            assert error.code == 400, error
        else:
            raise AssertionError(f"Accepted invalid database_id: {invalid}")


if __name__ == "__main__":
    main()
