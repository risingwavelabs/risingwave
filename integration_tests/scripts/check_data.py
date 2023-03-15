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
    run_sql("CREATE MATERIALIZED VIEW {0}_mv AS SELECT * FROM {0}".format(rel))


def check_mv(rel: str):
    rows = run_sql("SELECT COUNT(*) FROM {}_mv".format(rel))
    rows = int(rows.decode('utf8').strip())
    print("{} rows in {}".format(rows, rel))
    assert rows >= 1


def run_sql(sql):
    print("Running SQL: {}".format(sql))
    return subprocess.check_output(["psql", "-h", "localhost", "-p", "4566",
                                    "-d", "dev", "-U", "root", "--tuples-only", "-c", sql])


demo = sys.argv[1]
if demo in ['docker', 'iceberg-sink'] :
    print('Skip for running test for `%s`'%demo)
    sys.exit(0)
file_dir = dirname(abspath(__file__))
project_dir = dirname(file_dir)
demo_dir = os.path.join(project_dir, demo)
data_check = os.path.join(demo_dir, 'data_check')
with open(data_check) as f:
    relations = f.read().split(",")
    for rel in relations:
        create_mv(rel)
    time.sleep(20)
    for rel in relations:
        check_mv(rel)
