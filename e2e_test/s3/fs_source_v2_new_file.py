from fs_source_v2 import gen_data, FORMATTER, do_test
import json
import os
import random
import psycopg2
import time
from minio import Minio


def upload_to_s3_bucket(config, minio_client, run_id, files, start_bias):
    _local = lambda idx, start_bias: f"data_{idx + start_bias}.{fmt}"
    _s3 = lambda idx, start_bias: f"{run_id}_data_{idx + start_bias}.{fmt}"
    for idx, file_str in enumerate(files):
        with open(_local(idx, start_bias), "w") as f:
            f.write(file_str)
            os.fsync(f.fileno())

        minio_client.fput_object(
            config["S3_BUCKET"], _s3(idx, start_bias), _local(idx, start_bias)
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
            return True
        print(
            f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s"
        )
        time.sleep(10)
    return False


if __name__ == "__main__":
    FILE_NUM = 101
    ITEM_NUM_PER_FILE = 2
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)
    fmt = "json"

    split_idx = 51
    data_batch1 = data[:split_idx]
    data_batch2 = data[split_idx:]

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True,
    )
    run_id = str(random.randint(1000, 9999))
    print(f"S3 Source New File Test: run ID: {run_id} to bucket {config['S3_BUCKET']}")

    formatted_batch1 = FORMATTER[fmt](data_batch1)
    upload_to_s3_bucket(config, client, run_id, formatted_batch1, 0)

    # config in do_test that fs source's list interval is 1s
    do_test(
        config, len(data_batch1), ITEM_NUM_PER_FILE, run_id, fmt, need_drop_table=False
    )

    formatted_batch2 = FORMATTER[fmt](data_batch2)
    upload_to_s3_bucket(config, client, run_id, formatted_batch2, split_idx)

    success_flag = check_for_new_files(FILE_NUM, ITEM_NUM_PER_FILE, fmt)
    if success_flag:
        print("Test(add new file) pass")
    else:
        print("Test(add new file) fail")

    _s3 = lambda idx, start_bias: f"{run_id}_data_{idx + start_bias}.{fmt}"
    # clean up s3 files
    for idx, _ in enumerate(data):
        client.remove_object(config["S3_BUCKET"], _s3(idx, 0))

    if success_flag == False:
        exit(1)
