import os
import sys
import csv
import json
import random
import psycopg2
import opendal

from time import sleep
from io import StringIO
from functools import partial

def gen_data(file_num, item_num_per_file):
    assert item_num_per_file % 2 == 0, \
        f'item_num_per_file should be even to ensure sum(mark) == 0: {item_num_per_file}'
    return [
        [{
            'id': file_id * item_num_per_file + item_id,
            'name': f'{file_id}_{item_id}',
            'sex': item_id % 2,
            'mark': (-1) ** (item_id % 2),
        } for item_id in range(item_num_per_file)]
        for file_id in range(file_num)
    ]

def format_json(data):
    return [
        '\n'.join([json.dumps(item) for item in file])
        for file in data
    ]


def do_test(config, file_num, item_num_per_file, prefix, fmt, credential):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return f'gcs_test_{fmt}'

    def _encode():
        return 'JSON'

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE {_table()}(
        id int,
        name TEXT,
        sex int,
        mark int,
    ) WITH (
        connector = 'gcs',
        match_pattern = '{prefix}*.{fmt}',
        gcs.bucket_name = '{config['GCS_BUCKET']}',
        gcs.credentials = '{credential}',
    ) FORMAT PLAIN ENCODE {_encode()};''')

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
    FILE_NUM = 4001
    ITEM_NUM_PER_FILE = 2
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    fmt = sys.argv[1]
    FORMATTER = {
        'json': format_json,
    }
    assert fmt in FORMATTER, f"Unsupported format: {fmt}"
    formatted_files = FORMATTER[fmt](data)

    config = json.loads(os.environ["GCS_SOURCE_TEST_CONF"])
    run_id = str(random.randint(1000, 9999))
    _local = lambda idx: f'data_{idx}.{fmt}'
    _gcs = lambda idx: f"{run_id}_data_{idx}.{fmt}"
    credential_str = json.dumps(config["GOOGLE_APPLICATION_CREDENTIALS"])
    # put gcs files
    op = opendal.Operator("gcs", root="/", bucket=config["GCS_BUCKET"], credential=credential_str)

    print("upload file to gcs")
    for idx, file_str in enumerate(formatted_files):
        with open(_local(idx), "w") as f:
            f.write(file_str)
            os.fsync(f.fileno())
        file_bytes = file_str.encode('utf-8')
        op.write(_gcs(idx), file_bytes)

    # do test
    print("do test")
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt, credential_str)

    # clean up gcs files
    print("clean up gcs files")
    for idx, _ in enumerate(formatted_files):
        op.delete(_gcs(idx))
