#!/usr/bin/python3

from os.path import (dirname, abspath)
import os
import sys
import subprocess
from time import sleep
import argparse


def run_sql_file(f: str, dir: str):
    print("Running SQL file: {}".format(f))
    # ON_ERROR_STOP=1 will let psql return error code when the query fails.
    # https://stackoverflow.com/questions/37072245/check-return-status-of-psql-command-in-unix-shell-scripting
    proc = subprocess.run(["psql", "-h", "localhost", "-p", "4566",
                           "-d", "dev", "-U", "root", "-f", f, "-v", "ON_ERROR_STOP=1"], check=True,
                          cwd=dir)
    if proc.returncode != 0:
        sys.exit(1)


def run_demo(demo: str, format: str):
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: {}".format(demo))

    subprocess.run(["docker", "compose", "up", "-d", "-e",
    "ENABLE_TELEMETRY=false"], cwd=demo_dir, check=True)
    sleep(40)

    sql_files = ['create_source.sql', 'create_mv.sql', 'query.sql']
    for fname in sql_files:
        if format == 'protobuf':
            sql_file = os.path.join(demo_dir, "pb", fname)
            if os.path.isfile(sql_file):
                # Try to run the protobuf version first.
                run_sql_file(sql_file, demo_dir)
                sleep(10)
                continue
            # Fallback to default version when the protobuf version doesn't exist.
        sql_file = os.path.join(demo_dir,  fname)
        run_sql_file(sql_file, demo_dir)
        sleep(10)

def run_iceberg_demo():
    demo = "iceberg-sink"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: iceberg-sink")

    subprocess.run(["docker", "compose", "up", "-d", "-e", "ENABLE_TELEMETRY=false"], cwd=demo_dir, check=True)
    sleep(40)

    subprocess.run(["docker", "compose", "exec", "spark", "bash", "/spark-script/run-sql-file.sh", "create-table"],
                   cwd=demo_dir, check=True)

    sql_files = ['create_source.sql', 'create_mv.sql', 'create_sink.sql']
    for fname in sql_files:
        sql_file = os.path.join(demo_dir,  fname)
        print("executing sql: ", open(sql_file).read())
        run_sql_file(sql_file, demo_dir)
        sleep(10)

    print("sink created. Wait for 2 min time for ingestion")

    # wait for two minutes ingestion
    sleep(120)

    query_sql = open(os.path.join(demo_dir, "iceberg-query.sql")).read()

    print("querying iceberg with presto sql: %s"%query_sql)

    query_output_file_name = "query_outout.txt"

    query_output_file = open(query_output_file_name, "wb")

    subprocess.run(["docker", "compose", "exec", "presto", "presto-cli", "--server", "localhost:8080", "--execute", query_sql],
                   cwd=demo_dir, check=True, stdout=query_output_file)
    query_output_file.close()

    output_content = open(query_output_file_name).read()

    print(output_content)

    assert len(output_content.strip()) > 0




arg_parser = argparse.ArgumentParser(description='Run the demo')
arg_parser.add_argument('--format',
                        metavar='format',
                        type=str,
                        help='the format of output data',
                        default='json')
arg_parser.add_argument('--case',
                        metavar='case',
                        type=str,
                        help='the test case')
args = arg_parser.parse_args()

if args.case == "iceberg-sink":
    if args.format == "protobuf":
        print("skip protobuf test for iceberg-sink")
    else:
        run_iceberg_demo()
else:
    run_demo(args.case, args.format)
