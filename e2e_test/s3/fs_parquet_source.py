import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import psycopg2

from time import sleep
from io import StringIO
from minio import Minio
from functools import partial

def gen_data(file_num, row_nums):
    schema = pa.schema([
    ('id',  pa.int32()),
    ('name', pa.string()),    
    ('sex', pa.int32()),    
        ])
    id_data = [i for i in range(row_nums)]
    id_column = pa.array(ids_data, type=pa.int32())
    
    name_data = [f'name{i}_for_test'for i in range(row_nums)]
    name_column = pa.array(name_data, type=pa.string())
    
    sex_data = [i % 2 for i in range(row_nums)]
    sex_column = pa.array(sex_data, type=pa.int32())
    
    batch = pa.RecordBatch.from_arrays(
        [id_column, name_column, sex_column],
        schema = schema
    )
    table = pa.Table.from_batches([batch])
 
# 写 Parquet 文件 plain.parquet
pq.write_table(table, 'test_5.parquet')
    

def do_test(config, file_num, item_num_per_file, prefix, fmt):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return f's3_test_{fmt}'

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE {_table()}(
        id int,
        name TEXT,
        sex int,
        mark int,
    ) WITH (
        connector = 's3_v2',
        match_pattern = '{prefix}*.{fmt}',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
    ) FORMAT PLAIN ENCODE PARQUET;''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_table()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 30s")
        sleep(30)

    stmt = f'select count(*), sum(id), sum(sex), sum(mark) from {_table()}'
    print(f'Execute {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)
    _assert_eq('sum(sex)', result[2], total_rows / 2)
    _assert_eq('sum(mark)', result[3], 0)

    print('Test pass')

    cur.execute(f'drop table {_table()}')
    cur.close()
    conn.close()


if __name__ == "__main__":
    FILE_NUM = 3
    ITEM_NUM_PER_FILE = 10000
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    fmt = sys.argv[1]
    FORMATTER = {
        'parquet': format_json,
        'csv_with_header': partial(format_csv, with_header=True),
        'csv_without_header': partial(format_csv, with_header=False),
    }
    assert fmt in FORMATTER, f"Unsupported format: {fmt}"
    formatted_files = FORMATTER[fmt](data)

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True,
    )
    run_id = str(random.randint(1000, 9999))
    _local = lambda idx: f'data_{idx}.{fmt}'
    _s3 = lambda idx: f"{run_id}_data_{idx}.{fmt}"

    # put s3 files
    for idx, file_str in enumerate(formatted_files):
        with open(_local(idx), "w") as f:
            f.write(file_str)
            os.fsync(f.fileno())

        client.fput_object(
            config["S3_BUCKET"],
            _s3(idx),
            _local(idx)
        )

    # do test
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt)

    # clean up s3 files
    for idx, _ in enumerate(formatted_files):
        client.remove_object(config["S3_BUCKET"], _s3(idx))
