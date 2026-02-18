#!/usr/bin/python3

from os.path import (dirname, abspath)
import os
import sys
import subprocess
from time import sleep
import argparse


def run_sql_file(f: str, dir: str):
    print("Running SQL file: {} on RisingWave".format(f))
    # ON_ERROR_STOP=1 will let psql return error code when the query fails.
    # https://stackoverflow.com/questions/37072245/check-return-status-of-psql-command-in-unix-shell-scripting
    proc = subprocess.run(["psql", "-h", "localhost", "-p", "4566",
                           "-d", "dev", "-U", "root", "-f", f, "-v", "ON_ERROR_STOP=1", "-P", "pager=off"], check=True,
                          cwd=dir)
    if proc.returncode != 0:
        sys.exit(1)
    # Some demos historically relied on psql meta-commands like \sleep, which are not portable across psql versions.
    # For nats create_sink, add a short post-apply wait here to avoid flakiness without requiring \sleep in SQL.
    f_norm = f.replace('\\\\', '/').replace('\\', '/')
    if f_norm.endswith('integration_tests/nats/create_sink.sql') or '/nats/' in f_norm and f_norm.endswith('create_sink.sql'):
        sleep(int(os.getenv('NATS_SINK_WAIT_SECS', '5')))


def run_demo(demo: str, format: str, wait_time=40):
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: {}".format(demo))

    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=demo_dir, check=True)
    sleep(wait_time)

    prepare_file = 'prepare.sh'
    if os.path.exists(os.path.join(demo_dir, prepare_file)):
        subprocess.run(["bash", prepare_file], cwd=demo_dir, check=True)

    sql_files = ['create_source.sql', 'create_mv.sql', 'create_sink.sql', 'query.sql']
    for fname in sql_files:
        if format == 'protobuf':
            sql_file = os.path.join(demo_dir, "pb", fname)
            if os.path.isfile(sql_file):
                # Try to run the protobuf version first.
                run_sql_file(sql_file, demo_dir)
                sleep(10)
                continue
            # Fallback to default version when the protobuf version doesn't exist.
        sql_file = os.path.join(demo_dir, fname)
        if not os.path.exists(sql_file):
            continue
        run_sql_file(sql_file, demo_dir)
        sleep(10)


def iceberg_cdc_demo():
    demo = "iceberg-cdc"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: iceberg-cdc")
    subprocess.run(["bash", "./run_test.sh"], cwd=demo_dir, check=True)


def iceberg_sink_demo():
    demo = "iceberg-sink2"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: iceberg-sink2")
    subprocess.run(["bash", "./run.sh"], cwd=demo_dir, check=True)

def iceberg_source_demo():
    demo = "iceberg-source"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: iceberg-source")
    subprocess.run(["bash", "./run.sh"], cwd=demo_dir, check=True)


arg_parser = argparse.ArgumentParser(description="Run the demo")
arg_parser.add_argument(
    "--format",
    metavar="format",
    type=str,
    help="the format of output data",
    default="json",
)

arg_parser.add_argument("--case", metavar="case", type=str, help="the test case")
args = arg_parser.parse_args()

# disable telemetry in env
os.environ['ENABLE_TELEMETRY'] = "false"

if args.case == "iceberg-cdc":
    iceberg_cdc_demo()
elif args.case == "iceberg-sink":
    iceberg_sink_demo()
elif args.case == "iceberg-source":
    iceberg_source_demo()
else:
    run_demo(args.case, args.format)
