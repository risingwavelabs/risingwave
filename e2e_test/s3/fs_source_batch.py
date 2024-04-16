import os
import sys
import csv
import json
import random
import psycopg2

from time import sleep
from io import StringIO
from minio import Minio
from functools import partial
from .utils import gen_data, format_json, format_csv

def do_test(config, file_num, item_num_per_file, prefix, fmt):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _source():
        return f's3_test_{fmt}'

    def _encode():
        if fmt == 'json':
            return 'JSON'
        else:
            return f"CSV (delimiter = ',', without_header = {str('without' in fmt).lower()})"

    # Execute a SELECT statement
    cur.execute(f'''CREATE SOURCE {_source()}(
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
    ) FORMAT PLAIN ENCODE {_encode()};''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_source()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in source, {total_rows} expected, wait 30s")
        sleep(30)

    stmt = f'select count(*), sum(id), sum(sex), sum(mark) from {_source()}'
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

    cur.execute(f'drop source {_source()}')
    cur.close()
    conn.close()


if __name__ == "__main__":
    FILE_NUM = 4001
    ITEM_NUM_PER_FILE = 2
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    fmt = sys.argv[1]
    FORMATTER = {
        'json': format_json,
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
