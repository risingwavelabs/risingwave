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
                           "-d", "dev", "-U", "root", "-f", f, "-v", "ON_ERROR_STOP=1"], check=True,
                          cwd=dir)
    if proc.returncode != 0:
        sys.exit(1)


def run_demo(demo: str, format: str, wait_time = 40):
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: {}".format(demo))

    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=demo_dir, check=True)
    sleep(wait_time)

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

def run_kafka_cdc_demo():
    demo = "kafka-cdc-sink"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: kafka-cdc-sink")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=demo_dir, check=True)
    sleep(40)

    subprocess.run(["bash","./prepare.sh"], cwd=demo_dir, check=True)

    sql_files = ['create_source.sql', 'create_mv.sql', 'create_sink.sql']
    for fname in sql_files:
        sql_file = os.path.join(demo_dir,  fname)
        print("executing sql: ", open(sql_file).read())
        run_sql_file(sql_file, demo_dir)

    print("sink created. Wait for 2 min time for ingestion")

    # wait for two minutes ingestion
    sleep(120)

    pg_check_file = os.path.join(demo_dir, 'pg_check')
    with open(pg_check_file) as f:
        relations = f.read().strip().split(",")
        failed_cases = []
        for rel in relations:
            sql = 'SELECT COUNT(*) FROM {}'.format(rel)
            print("Running SQL: {} on PG".format(sql))
            command = 'psql -U $POSTGRES_USER $POSTGRES_DB --tuples-only -c "{}"'.format(sql)
            rows = subprocess.check_output(["docker", "exec", "postgres", "bash", "-c", command])
            rows = int(rows.decode('utf8').strip())
            print("{} rows in {}".format(rows, rel))
            if rows < 1:
                failed_cases.append(rel)
        if len(failed_cases) != 0:
            raise Exception("Data check failed for case {}".format(failed_cases))

def iceberg_cdc_demo():
    demo = "iceberg-cdc"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: iceberg-cdc")
    subprocess.run(["bash","./run_test.sh"], cwd=demo_dir, check=True)

def run_iceberg_demo():
    demo = "iceberg-sink"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: iceberg-sink")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=demo_dir, check=True)
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

    print("querying iceberg with presto sql: %s" % query_sql)

    query_output_file_name = "query_outout.txt"

    query_output_file = open(query_output_file_name, "wb")

    subprocess.run(["docker", "compose", "exec", "presto", "presto-cli", "--server", "localhost:8080", "--execute", query_sql],
                   cwd=demo_dir, check=True, stdout=query_output_file)
    query_output_file.close()

    output_content = open(query_output_file_name).read()

    print(output_content)

    assert len(output_content.strip()) > 0

def run_clickhouse_demo():
    demo = "clickhouse-sink"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: clickhouse-sink")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=demo_dir, check=True)
    sleep(40)


    subprocess.run(["docker", "compose", "exec", "clickhouse-server", "bash", "/opt/clickhouse/clickhouse-sql/run-sql-file.sh", "create_clickhouse_table"],
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

    query_output_file_name = "query_outout.txt"

    query_output_file = open(query_output_file_name, "wb")

    subprocess.run(["docker", "compose", "exec", "clickhouse-server", "bash", "/opt/clickhouse/clickhouse-sql/run-sql-file.sh", "clickhouse_query"],
                   cwd=demo_dir, check=True, stdout=query_output_file)
    query_output_file.close()

    output_content = open(query_output_file_name).read()

    print(output_content)

    assert len(output_content.strip()) > 0

def run_cassandra_and_scylladb_sink_demo():
    demo = "cassandra-and-scylladb-sink"
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Running demo: {}".format(demo))

    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=demo_dir, check=True)
    print("wait two min for cassandra and scylladb to start up")
    sleep(120)

    dbs = ['cassandra', 'scylladb']
    for db in dbs:
        subprocess.run(["docker", "compose", "exec", db, "cqlsh", "-f", "prepare_cassandra_and_scylladb.sql"], cwd=demo_dir, check=True)

    sql_files = ['create_source.sql', 'create_mv.sql', 'create_sink.sql']
    for fname in sql_files:
        sql_file = os.path.join(demo_dir,  fname)
        print("executing sql: ", open(sql_file).read())
        run_sql_file(sql_file, demo_dir)

    print("sink created. Wait for 1 min time for ingestion")

    # wait for one minutes ingestion
    sleep(60)

    sink_check_file = os.path.join(demo_dir, 'sink_check')
    with open(sink_check_file) as f:
        relations = f.read().strip().split(",")
        failed_cases = []
        for rel in relations:
            sql = 'select count(*) from {};'.format(rel)
            for db in dbs:
                print("Running SQL: {} on {}".format(sql, db))
                query_output_file_name = os.path.join(demo_dir, "query_{}_outout.txt".format(db))
                query_output_file = open(query_output_file_name, "wb+")

                command = "docker compose exec scylladb cqlsh -e"
                subprocess.run(["docker", "compose", "exec", db, "cqlsh", "-e", sql], cwd=demo_dir, check=True, stdout=query_output_file)

                # output file:
                #
                #  count
                # -------
                #   1000
                #
                # (1 rows)
                query_output_file.seek(0)
                lines = query_output_file.readlines()
                query_output_file.close()
                assert len(lines) >= 6
                assert lines[1].decode('utf-8').strip().lower() == 'count'
                rows = int(lines[3].decode('utf-8').strip())
                print("{} rows in {}.{}".format(rows, db, rel))
                if rows < 1:
                    failed_cases.append(db + "_" + rel)
        if len(failed_cases) != 0:
            raise Exception("Data check failed for case {}".format(failed_cases))

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

# disable telemetry in env
os.environ['ENABLE_TELEMETRY'] = "false"

if args.case == "iceberg-sink":
    if args.format == "protobuf":
        print("skip protobuf test for iceberg-sink")
    else:
        run_iceberg_demo()
elif args.case == "clickhouse-sink":
    run_clickhouse_demo()
elif args.case == "iceberg-cdc":
    iceberg_cdc_demo()
elif args.case == "kafka-cdc-sink":
    run_kafka_cdc_demo()
elif args.case == "cassandra-and-scylladb-sink":
    run_cassandra_and_scylladb_sink_demo()
else:
    run_demo(args.case, args.format)
