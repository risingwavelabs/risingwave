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

def gen_data(file_num, item_num_per_file):
    assert item_num_per_file % 2 == 0, \
        f'item_num_per_file should be even to ensure sum(mark) == 0: {item_num_per_file}'
    return [
        [{
            'id': file_id * item_num_per_file + item_id,
            'name': f'{file_id}_{item_id}_{file_id * item_num_per_file + item_id}',
            'sex': item_id % 2,
            'mark': (-1) ** (item_id % 2),
            'test_int': pa.scalar(1, type=pa.int32()),
            'test_real': pa.scalar(4.0, type=pa.float32()),
            'test_double_precision': pa.scalar(5.0, type=pa.float64()),
            'test_varchar': pa.scalar('7', type=pa.string()),
            'test_bytea': pa.scalar(b'\xDe00BeEf', type=pa.binary()),
            'test_date': pa.scalar(datetime.now().date(), type=pa.date32()),
            'test_time': pa.scalar(datetime.now().time(), type=pa.time64('us')),
            'test_timestamp': pa.scalar(datetime.now().timestamp() * 1000000, type=pa.timestamp('us')),
            'test_timestamptz': pa.scalar(datetime.now().timestamp() * 1000, type=pa.timestamp('us', tz='+00:00')),
        } for item_id in range(item_num_per_file)]
        for file_id in range(file_num)
    ]

def do_test(config, file_num, item_num_per_file, prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return 's3_test_parquet'

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE {_table()}(
        id bigint primary key,
        name TEXT,
        sex bigint,
        mark bigint,
        test_int int,
        test_real real,
        test_double_precision double precision,
        test_varchar varchar,
        test_bytea bytea,
        test_date date,
        test_time time,
        test_timestamp timestamp,
        test_timestamptz timestamptz,
    ) WITH (
        connector = 's3',
        match_pattern = '*.parquet',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}',
        refresh.interval.sec = 1,
    ) FORMAT PLAIN ENCODE PARQUET;''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_table()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s")
        sleep(10)

    stmt = f'select count(*), sum(id) from {_table()}'
    print(f'Execute {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)

    print('File source test pass!')

    cur.close()
    conn.close()

def do_sink(config, file_num, item_num_per_file, prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return 's3_test_parquet'

    # Execute a SELECT statement
    cur.execute(f'''set sink_decouple = false;''')
    cur.execute(f'''CREATE sink test_file_sink as select
        id,
        name,
        sex,
        mark,
        test_int,
        test_real,
        test_double_precision,
        test_varchar,
        test_bytea,
        test_date,
        test_time,
        test_timestamp,
        test_timestamptz
        from {_table()} WITH (
        connector = 's3',
        match_pattern = '*.parquet',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}',
        s3.path = '',
        s3.file_type = 'parquet',
        type = 'append-only',
        force_append_only='true'
    ) FORMAT PLAIN ENCODE PARQUET(force_append_only='true');''')

    print('Sink into s3...')
    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE test_sink_table(
        id bigint primary key,
        name TEXT,
        sex bigint,
        mark bigint,
        test_int int,
        test_real real,
        test_double_precision double precision,
        test_varchar varchar,
        test_bytea bytea,
        test_date date,
        test_time time,
        test_timestamp timestamp,
        test_timestamptz timestamptz,
    ) WITH (
        connector = 's3',
        match_pattern = '*.parquet',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
    ) FORMAT PLAIN ENCODE PARQUET;''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from test_sink_table')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s")
        sleep(10)

    stmt = f'select count(*), sum(id) from test_sink_table'
    print(f'Execute reading sink files: {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)

    print('File sink test pass!')
    cur.execute(f'drop sink test_file_sink')
    cur.execute(f'drop table test_sink_table')
    cur.close()
    conn.close()



if __name__ == "__main__":
    FILE_NUM = 10
    ITEM_NUM_PER_FILE = 2000
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True,
    )
    run_id = str(random.randint(1000, 9999))
    _local = lambda idx: f'data_{idx}.parquet'
    _s3 = lambda idx: f"{run_id}_data_{idx}.parquet"

    # put s3 files
    for idx, file_data in enumerate(data):
        table = pa.Table.from_pandas(pd.DataFrame(file_data))
        pq.write_table(table, _local(idx))

        client.fput_object(
            config["S3_BUCKET"],
            _s3(idx),
            _local(idx)
        )

    # do test
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id)

    # clean up s3 files
    for idx, _ in enumerate(data):
       client.remove_object(config["S3_BUCKET"], _s3(idx))

    do_sink(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id)

     # clean up s3 files
    for idx, _ in enumerate(data):
       client.remove_object(config["S3_BUCKET"], _s3(idx))

