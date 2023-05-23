#!/usr/bin/python3

# Every demo directory contains a 'data_check' file that lists the relations (either source or mv)
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
    assert rows >= 1


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

    # don't know why if query upstream with `mysql` or `psql` command it will stuck,
    # so just check the count approximately.
    print("All upstream data has been loaded into RisingWave: {}".format(mv_rows))
    assert mv_rows >= 500000


def run_psql(sql):
    print("Running SQL: {} on RisingWave".format(sql))
    return subprocess.check_output(["psql", "-h", "localhost", "-p", "4566",
                                    "-d", "dev", "-U", "root", "--tuples-only", "-c", sql])


demo = sys.argv[1]
upstream = sys.argv[2]  # mysql, postgres, etc. see scripts/integration_tests.sh
if demo in ['docker', 'iceberg-sink']:
    print('Skip for running test for `%s`' % demo)
    sys.exit(0)

file_dir = dirname(abspath(__file__))
project_dir = dirname(file_dir)
demo_dir = os.path.join(project_dir, demo)
data_check_file = os.path.join(demo_dir, 'data_check')
with open(data_check_file) as f:
    relations = f.read().strip().split(",")
    for rel in relations:
        create_mv(rel)
    time.sleep(20)
    for rel in relations:
        check_mv(rel)

cdc_check_file = os.path.join(demo_dir, 'cdc_check')
if not os.path.exists(cdc_check_file):
    print("Skip cdc check for {}".format(demo))
    sys.exit(0)

with open(cdc_check_file) as f:
    print("Check cdc table with upstream {}".format(upstream))
    relations = f.read().strip().split(",")
    for rel in relations:
        check_cdc_table(rel)
