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


# compare the number of rows with upstream
def check_data_with_upstream(rel: str, upstream: str):
    # check type of upstream
    count_sql = "SELECT COUNT(*) FROM {}".format(rel)
    if upstream == "mysql":
        upstream_rows = run_mysql_upstream(count_sql)
    elif upstream == "postgres":
        upstream_rows = run_psql_upstream(count_sql)
    else:
        raise Exception("Unsupported upstream: {}".format(upstream))
    expect_rows = int(upstream_rows.decode('utf8').strip())
    actual_rows = int(run_psql(count_sql).decode('utf8').strip())

    print("Expect {} rows, actual {} rows".format(expect_rows, actual_rows))
    assert expect_rows == actual_rows


def run_mysql_upstream(sql):
    print("Running SQL: {}".format(sql))
    return subprocess.check_output(["mysql", "-h", "mysql", "-P", "3306", "-D", "mydb",
                                    "-u", "root", "-p", "123456", "-Ne", sql, "-B"])


def run_psql_upstream(sql):
    print("Running SQL: {}".format(sql))
    return subprocess.check_output(["psql", "-h", "postgres", "-p", "5432",
                                    "-d", "mydb", "-U", "myuser", "--tuples-only", "-c", sql])


def run_psql(sql):
    print("Running SQL: {}".format(sql))
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
    for line in f.readlines():
        if line.startswith('relations:'):
            start = line.index(' ')
            relations = line[start:].strip().split(",")
            for rel in relations:
                create_mv(rel)
            time.sleep(20)
            for rel in relations:
                check_mv(rel)
        elif line.startswith('cdc-tables:'):
            start = line.index(' ')
            relations = line[start:].strip().split(",")
            for rel in relations:
                check_data_with_upstream(rel, upstream)
