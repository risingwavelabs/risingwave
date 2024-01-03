import os
import sys
import csv
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

def format_csv(data, with_header):
    csv_files = []

    for file_data in data:
        ostream = StringIO()
        writer = csv.DictWriter(ostream, fieldnames=file_data[0].keys())
        if with_header:
            writer.writeheader()
        for item_data in file_data:
            writer.writerow(item_data)
        csv_files.append(ostream.getvalue())
    return csv_files


def do_test(file_num, item_num_per_file, prefix, fmt):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return f'posix_fs_test_{fmt}'

    def _encode():
        return f"CSV (delimiter = ',', without_header = {str('without' in fmt).lower()})"

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE {_table()}(
        id int,
        name TEXT,
        sex int,
        mark int,
    ) WITH (
        connector = 'posix_fs',
        match_pattern = '{prefix}*.{fmt}',
        posix_fs.root = '/tmp',
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
        'csv_with_header': partial(format_csv, with_header=True),
        'csv_without_header': partial(format_csv, with_header=False),
    }
    assert fmt in FORMATTER, f"Unsupported format: {fmt}"
    formatted_files = FORMATTER[fmt](data)

    run_id = str(random.randint(1000, 9999))
    _local = lambda idx: f'data_{idx}.{fmt}'
    _posix = lambda idx: f"{run_id}_data_{idx}.{fmt}"
    # put local files
    op = opendal.Operator("fs", root="/tmp")

    print("write file to /tmp")
    for idx, file_str in enumerate(formatted_files):
        with open(_local(idx), "w") as f:
            f.write(file_str)
            os.fsync(f.fileno())
        file_name = _posix(idx)
        print(f"Wrote {file_name} to /tmp")
        file_bytes = file_str.encode('utf-8')
        op.write(file_name, file_bytes)

    # do test
    print("do test")
    do_test(FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt)

    # clean up local files
    print("clean up local files in /tmp")
    for idx, _ in enumerate(formatted_files):
        file_name = _posix(idx)
        print(f"Removed {file_name} from /tmp")
        op.delete(file_name)
