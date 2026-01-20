import os
import sys
import csv
import json
import random
import psycopg2

# from handle_incremental_file import upload_to_s3_bucket, check_for_new_files
from time import sleep
import time
from io import StringIO
from minio import Minio
import gzip
from functools import partial
import pyarrow as pa
import pyarrow.parquet as pq


def _test_parquet_struct_field_subset(minio_client: Minio):
    """Generate a Parquet file with struct superset fields and verify RW can read a subset.

    Upstream Parquet schema:
      - id: int32
      - s: struct<foo:int32, bar:string, baz:int32>

    RisingWave table schema (subset):
      - id: int
      - s: struct<foo:int, bar:varchar>
    """

    # 1) Generate Parquet locally.
    run_id = str(random.randint(1000, 9999))
    local_path = f"parquet_struct_subset_{run_id}.parquet"
    s3_key = f"test_parquet_struct_subset/{run_id}.parquet"

    id_arr = pa.array([1, 2], type=pa.int32())
    foo = pa.array([10, 20], type=pa.int32())
    bar = pa.array(["a", "b"], type=pa.string())
    baz = pa.array([100, 200], type=pa.int32())
    s_arr = pa.StructArray.from_arrays([foo, bar, baz], names=["foo", "bar", "baz"])

    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field(
                "s",
                pa.struct(
                    [
                        pa.field("foo", pa.int32(), nullable=True),
                        pa.field("bar", pa.string(), nullable=True),
                        pa.field("baz", pa.int32(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    table = pa.Table.from_arrays([id_arr, s_arr], schema=schema)
    pq.write_table(table, local_path, compression="snappy", version="2.6")

    # 2) Upload to MinIO.
    minio_client.fput_object(
        bucket_name="hummock001",
        object_name=s3_key,
        file_path=local_path,
    )

    # 3) Create RW table that only reads subset fields (foo, bar).
    conn = psycopg2.connect(host="localhost", port="4566", user="root", database="dev")
    cur = conn.cursor()

    table_name = "s3_test_parquet_struct_subset"
    cur.execute(f"drop table if exists {table_name};")
    cur.execute(
        f"""
CREATE TABLE {table_name}(
  id int,
  s STRUCT<foo INT, bar VARCHAR>
) WITH (
  connector = 's3',
  match_pattern = '{s3_key}',
  s3.region_name = 'custom',
  s3.bucket_name = 'hummock001',
  s3.credentials.access = 'hummockadmin',
  s3.credentials.secret = 'hummockadmin',
  s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
  refresh.interval.sec = 1
) FORMAT PLAIN ENCODE PARQUET;
"""
    )

    # Wait for file to be ingested.
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f"select count(*) from {table_name}")
        result = cur.fetchone()
        if result[0] == 2:
            break
        print(
            f"[retry {retry_no}] Now got {result[0]} rows in table, 2 expected, wait 5s"
        )
        sleep(5)

    # Verify we can read subset fields from struct.
    cur.execute(f"select id, s.foo, s.bar from {table_name} order by id")
    rows = cur.fetchall()
    assert rows == [(1, 10, "a"), (2, 20, "b")], f"unexpected rows: {rows}"
    print("Test parquet struct subset read pass")

    # Cleanup RW & MinIO & local.
    cur.execute(f"drop table {table_name};")
    cur.close()
    conn.close()

    minio_client.remove_object(bucket_name="hummock001", object_name=s3_key)
    try:
        os.remove(local_path)
    except OSError:
        pass

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

def do_test(config, file_num, item_num_per_file, prefix, fmt, need_drop_table=True, dir="", compress=".gz"):
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

    def _encode():
        if fmt == 'json':
            return 'JSON'
        else:
            return f"CSV (delimiter = ',', without_header = {str('without' in fmt).lower()})"

    def _include_clause():
        if fmt == 'json':
            return 'INCLUDE payload as rw_payload'
        else:
            return ''

    # Execute a SELECT statement
    if fmt == 'json':
        cur.execute(f'''CREATE TABLE {_table()}(
            id int,
            name TEXT,
            sex int,
            mark int,
        )
        {_include_clause()}
        WITH (
            connector = 's3',
            match_pattern = '{dir}*.{fmt}{compress}',
            s3.region_name = 'custom',
            s3.bucket_name = 'hummock001',
            s3.credentials.access = 'hummockadmin',
            s3.credentials.secret = 'hummockadmin',
            s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
            refresh.interval.sec = 1,
        ) FORMAT PLAIN ENCODE {_encode()};''')
    else:
        cur.execute(f'''CREATE TABLE {_table()}(
            id int,
            name TEXT,
            sex int,
            mark int,
        )
        {_include_clause()}
        WITH (
            connector = 's3',
            match_pattern =  '{dir}*.{fmt}',
            s3.region_name = 'custom',
            s3.bucket_name = 'hummock001',
            s3.credentials.access = 'hummockadmin',
            s3.credentials.secret = 'hummockadmin',
            s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
            refresh.interval.sec = 1,
        ) FORMAT PLAIN ENCODE {_encode()};''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_table()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s")
        sleep(10)

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

    # only do payload check for json format, which enables INCLUDE CLAUSE
    if fmt == 'json':
        # check rw_payload
        print('Check rw_payload')
        stmt = f"select id, name, sex, mark, rw_payload from {_table()} limit 1;"
        cur.execute(stmt)
        result = cur.fetchone()
        print("Got one line with rw_payload: ", result)
        payload = result[4]
        _assert_eq('id', payload['id'], result[0])
        _assert_eq('name', payload['name'], result[1])
        _assert_eq('sex', payload['sex'], result[2])
        _assert_eq('mark', payload['mark'], result[3])

        print('Test pass')

    if need_drop_table:
        cur.execute(f'drop table {_table()}')
    cur.close()
    conn.close()

FORMATTER = {
        'json': format_json,
        'csv_with_header': partial(format_csv, with_header=True),
        'csv_without_header': partial(format_csv, with_header=False),
    }


def test_batch_read(config, file_num, item_num_per_file, prefix, fmt, dir = "", compress=".gz"):
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
    if fmt == 'json':
        cur.execute(f'''CREATE SOURCE {_source()}(
            id int,
            name TEXT,
            sex int,
            mark int,
        ) WITH (
            connector = 's3',
            match_pattern =  '{dir}*.{fmt}{compress}',
            s3.region_name = 'custom',
            s3.bucket_name = 'hummock001',
            s3.credentials.access = 'hummockadmin',
            s3.credentials.secret = 'hummockadmin',
            s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
            refresh.interval.sec = 1
        ) FORMAT PLAIN ENCODE {_encode()};''')
    else:
        cur.execute(f'''CREATE SOURCE {_source()}(
            id int,
            name TEXT,
            sex int,
            mark int,
        ) WITH (
            connector = 's3',
            match_pattern =  '{dir}{prefix}*.{fmt}',
            s3.region_name = 'custom',
            s3.bucket_name = 'hummock001',
            s3.credentials.access = 'hummockadmin',
            s3.credentials.secret = 'hummockadmin',
            s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
            refresh.interval.sec = 1
        ) FORMAT PLAIN ENCODE {_encode()};''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_source()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in source, {total_rows} expected, wait 30s")
        sleep(10)

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

    print('Test batch read pass')

    cur.execute(f'drop source {_source()}')
    cur.close()
    conn.close()


def upload_to_s3_bucket(config, minio_client, run_id, files, start_bias, dir):
    _local = lambda idx, start_bias: f"data_{idx + start_bias}.{fmt}"
    _s3 = lambda idx, start_bias: f"/{dir}/{run_id}_data_{idx + start_bias}.{fmt}"
    for idx, file_str in enumerate(files):
        with open(_local(idx, start_bias), "w") as f:
            f.write(file_str)
            os.fsync(f.fileno())

        minio_client.fput_object(
            bucket_name="hummock001",
            object_name=_s3(idx, start_bias),
            file_path=_local(idx, start_bias),
        )


def check_for_new_files(file_num, item_num_per_file, fmt):
    conn = psycopg2.connect(host="localhost", port="4566", user="root", database="dev")

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return f"s3_test_{fmt}"

    total_rows = file_num * item_num_per_file

    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f"select count(*) from {_table()}")
        result = cur.fetchone()
        if result[0] == total_rows:
            cur.execute(f'drop table {_table()};')
            return True
        print(
            f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s"
        )
        time.sleep(10)
    return False


if __name__ == "__main__":
    FILE_NUM = 2000
    ITEM_NUM_PER_FILE = 4
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    fmt = sys.argv[1]
    assert fmt in FORMATTER, f"Unsupported format: {fmt}"
    formatted_files = FORMATTER[fmt](data)

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        endpoint="127.0.0.1:9301",
        access_key="hummockadmin",
        secret_key="hummockadmin",
        secure=False,
    )
    run_id = str(random.randint(1000, 9999))

    _local = lambda idx: f'data_{idx}.{fmt}'
    if fmt == 'json':
        _s3 = lambda idx: f"{run_id}_data_{idx}.{fmt}.gz"
        for idx, file_str in enumerate(formatted_files):
            with open(_local(idx), "w") as f:
                with gzip.open(_local(idx) + '.gz', 'wb') as f_gz:
                    f_gz.write(file_str.encode('utf-8'))
                    os.fsync(f.fileno())

            client.fput_object(
                bucket_name="hummock001",
                object_name=_s3(idx),
                file_path=_local(idx) + '.gz',
            )
    else:
        _s3 = lambda idx: f"{run_id}_data_{idx}.{fmt}"
        for idx, file_str in enumerate(formatted_files):
            with open(_local(idx), "w") as f:
                f.write(file_str)
                os.fsync(f.fileno())

            client.fput_object(
                bucket_name="hummock001",
                object_name=_s3(idx),
                file_path=_local(idx),
            )

    # do test
    print("Test streaming file source...\n")
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt)

    print("Test batch read file source...\n")
    test_batch_read(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt)

    _s3 = lambda idx, start_bias: (
        f"{run_id}_data_{idx + start_bias}.{fmt}"
        if fmt != 'json' else
        f"{run_id}_data_{idx}.{fmt}.gz"
    )
    # clean up s3 files
    for idx, _ in enumerate(formatted_files):
        client.remove_object(bucket_name="hummock001", object_name=_s3(idx, 0))

    # test file source handle incremental files
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)
    fmt = "json"

    split_idx = 51
    data_batch1 = data[:split_idx]
    data_batch2 = data[split_idx:]
    run_id = str(random.randint(1000, 9999))
    print(f"S3 Source New File Test: run ID: {run_id} to bucket")

    formatted_batch1 = FORMATTER[fmt](data_batch1)
    upload_to_s3_bucket(config, client, run_id, formatted_batch1, 0, "test_incremental/")
    _s3_incremental = lambda idx, start_bias: f"test_incremental/{run_id}_data_{idx + start_bias}.{fmt}"

    # config in do_test that fs source's list interval is 1s
    do_test(
        config, len(data_batch1), ITEM_NUM_PER_FILE, run_id, fmt, need_drop_table=False, dir="test_incremental/", compress="")

    formatted_batch2 = FORMATTER[fmt](data_batch2)
    upload_to_s3_bucket(config, client, run_id, formatted_batch2, split_idx, "test_incremental/")

    success_flag = check_for_new_files(FILE_NUM, ITEM_NUM_PER_FILE, fmt)
    if success_flag:
        print("Test(add new file) pass")
    else:
        print("Test(add new file) fail")

    _s3 = lambda idx, start_bias: f"test_incremental/{run_id}_data_{idx + start_bias}.{fmt}"
    # clean up s3 files in test_incremental dir
    for idx, _ in enumerate(formatted_files):
        client.remove_object(bucket_name="hummock001", object_name=_s3(idx, 0))

    # Parquet struct subset read test.
    print("Test parquet struct subset read...\n")
    _test_parquet_struct_field_subset(client)
