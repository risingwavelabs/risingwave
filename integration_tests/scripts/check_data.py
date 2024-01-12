#!/usr/bin/python3

# Every sink demo directory contains a 'sink_check.py' file that used to check test,
# and other demo directory contains a 'data_check' file that lists the relations (either source or mv)
# that are expected to have >=1 rows. This script runs the checks by creating a materialized view over it,
# and verify the rows count in the view.

import os
from posixpath import abspath
import subprocess
import sys

from os.path import dirname
import time


def create_mv(rel: str):
    if "_mv" in rel:
        raise Exception('relation "{}" must not contains "_mv"'.format(rel))
    run_psql("CREATE MATERIALIZED VIEW {0}_mv AS SELECT * FROM {0}".format(rel))


def check_mv(rel: str):
    rows = run_psql("SELECT COUNT(*) FROM {}_mv".format(rel))
    rows = int(rows.decode('utf8').strip())
    print("{} rows in {}".format(rows, rel))
    return rows >= 1


# Check the number of rows of cdc table
def check_cdc_table(rel: str):
    print("Wait for all upstream data to be available in RisingWave")
    mv_count_sql = "SELECT * FROM {}_count".format(rel)
    mv_rows = 0
    rows = run_psql(mv_count_sql)
    rows = int(rows.decode('utf8').strip())
    while rows > mv_rows:
        print("Current row count: {}".format(rows))
        mv_rows = rows
        time.sleep(60)
        rows = run_psql(mv_count_sql)
        rows = int(rows.decode('utf8').strip())

    # don't know why if query upstream with `mysql` or `psql` command it will get stuck,
    # so just check the count approximately. maybe due to bad cpu and disk of the spot instance
    print("All upstream data (roughly) has been loaded into RisingWave: {}".format(mv_rows))
    assert mv_rows >= 200000


def run_psql(sql):
    print("Running SQL: {} on RisingWave".format(sql))
    return subprocess.check_output(["psql", "-h", "localhost", "-p", "4566",
                                    "-d", "dev", "-U", "root", "--tuples-only", "-c", sql])


def data_check(data_check_file: str):
    with open(data_check_file) as f:
        relations = f.read().strip().split(",")
        for rel in relations:
            create_mv(rel)
            time.sleep(20)
        failed_cases = []
        for rel in relations:
            if not check_mv(rel):
                failed_cases.append(rel)
        if len(failed_cases) != 0:
            raise Exception("Data check failed for case {}".format(failed_cases))


def sink_check(demo_dir: str, sink_check_file: str):
    print("sink created. Wait for half min time for ingestion")

    # wait for half min ingestion
    time.sleep(30)
    subprocess.run(["python3", sink_check_file], cwd=demo_dir, check=True)


def cdc_check(cdc_check_file: str, upstream: str):
    with open(cdc_check_file) as f:
        print("Check cdc table with upstream {}".format(upstream))
        relations = f.read().strip().split(",")
        for rel in relations:
            check_cdc_table(rel)


def test_check(demo: str, upstream: str, need_data_check=True, need_sink_check=False):
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)

    data_check_file = os.path.join(demo_dir, 'data_check')
    if need_data_check or os.path.exists(data_check_file):
        data_check(data_check_file)
    else:
        print(f"skip data check for {demo}")

    sink_check_file = os.path.join(demo_dir, 'sink_check.py')
    if need_sink_check or os.path.exists(sink_check_file):
        sink_check(demo_dir, sink_check_file)
    else:
        print(f"skip sink check for {demo}")

    cdc_check_file = os.path.join(demo_dir, 'cdc_check')
    if os.path.exists(cdc_check_file):
        cdc_check(cdc_check_file, upstream)
    else:
        print(f"skip cdc check for {demo}")


demo = sys.argv[1]
upstream = sys.argv[2]  # mysql, postgres, etc. see scripts/integration_tests.sh
if demo in ['docker', 'iceberg-cdc']:
    print('Skip for running test for `%s`' % demo)
    sys.exit(0)

if 'sink' in demo:
    test_check(demo, upstream, need_data_check=False, need_sink_check=True)
else:
    test_check(demo, upstream, need_data_check=True, need_sink_check=False)
