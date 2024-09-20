import os
import sys
import random
import psycopg2
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timezone
from time import sleep
from minio import Minio
from random import uniform


def do_test(config, file_num, item_num_per_file, prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()


    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE t (v1 int, v2 int);''')

    cur.execute(f'''CREATE sink test_file_sink_batching as select
        v1, v2 from t WITH (
        connector = 's3',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
        s3.path = 'test_sink/',
        s3.file_type = 'parquet',
        type = 'append-only',
        rollover_seconds = '10',
        max_row_count = '5',
        force_append_only='true'
    ) FORMAT PLAIN ENCODE PARQUET(force_append_only='true');''')
    
    cur.execute(f'''CREATE TABLE test_sink_table(
        v1 int,
        v2 int,
    ) WITH (
        connector = 's3',
        match_pattern = 'test_sink/*.parquet',
        refresh_interval_sec = '1',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
    ) FORMAT PLAIN ENCODE PARQUET;''')
    
    cur.execute(f'''INSERT INTO t VALUES (10, 10);''')
    cur.execute(f'select count(*) from test_sink_table')
    # no item will be selected
    result = cur.fetchone()
    
    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'
        
    _assert_eq('count(*)', result[0], 0)
    time.sleep(10)
    
    _assert_eq('count(*)', result[0], 1)

    

    cur.close()
    conn.close()


if __name__ == "__main__":
    FILE_NUM = 10
    ITEM_NUM_PER_FILE = 2000

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        "127.0.0.1:9301",
        "hummockadmin",
        "hummockadmin",
        secure=False,
    )
    run_id = str(random.randint(1000, 9999))
    _local = lambda idx: f'data_{idx}.parquet'
    _s3 = lambda idx: f"{run_id}_data_{idx}.parquet"


    # do test
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id)

    # clean up s3 files
    for idx, _ in enumerate(data):
       client.remove_object("hummock001", _s3(idx))



