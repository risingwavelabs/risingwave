import os
import string
import json
import string
from time import sleep
from minio import Minio
import psycopg2
import random


def do_test(config, N, n, prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()
    cur.execute(f'''CREATE TABLE s3_test_csv_without_headers( 
        a int,
        b int,
        c int,
    ) WITH (
        connector = 's3',
        match_pattern = '{prefix}_data_without_headers.csv',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
    ) ROW FORMAT CSV WITHOUT HEADER DELIMITED BY ',';''')

    cur.execute(f'''CREATE TABLE s3_test_csv_with_headers( 
        a int,
        b int,
        c int,
    ) WITH (
        connector = 's3',
        match_pattern = '{prefix}_data_with_headers.csv',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
    ) ROW FORMAT CSV DELIMITED BY ',';''')

    total_row = int(N * n)
    sleep(60)
    while True:
        sleep(60)
        cur.execute('select count(*) from s3_test_csv_with_headers')
        result_with_headers = cur.fetchone()
        cur.execute('select count(*) from s3_test_csv_without_headers')
        result_without_headers = cur.fetchone()
        if result_with_headers[0] == total_row and result_without_headers[0] == total_row:
            break
        print(
            f"Now got {result_with_headers[0]} rows in table, {total_row} expected, wait 60s")

    cur.execute(
        'select count(*), sum(a), sum(b), sum(c) from s3_test_csv_with_headers')
    result_with_headers = cur.fetchone()

    cur.execute(
        'select count(*), sum(a), sum(b), sum(c) from s3_test_csv_without_headers')
    s3_test_csv_without_headers = cur.fetchone()

    print(result_with_headers, s3_test_csv_without_headers,
          int(((N - 1) * N / 2) * n), int(N*n / 2))

    assert s3_test_csv_without_headers[0] == total_row
    assert s3_test_csv_without_headers[1] == int(((N - 1) * N / 2) * n)
    assert s3_test_csv_without_headers[2] == int(N*n / 2)
    assert s3_test_csv_without_headers[3] == 0

    assert result_with_headers[0] == total_row
    assert result_with_headers[1] == 0
    assert result_with_headers[2] == int(N*n / 2)
    assert result_with_headers[3] == int(((N - 1) * N / 2) * n)

    cur.execute('drop table s3_test_csv_with_headers')
    cur.execute('drop table s3_test_csv_without_headers')

    cur.close()
    conn.close()


if __name__ == "__main__":
    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    run_id = str(random.randint(1000, 9999))
    N = 10000
    # do_test(config, N, 0, run_id)
    items = [",".join([str(j), str(j % 2), str(-1 if j % 2 else 1)])
             for j in range(N)
             ]

    data = "\n".join(items) + "\n"
    n = 10
    with open("data_without_headers.csv", "w") as f:
        for _ in range(10):
            f.write(data)
        os.fsync(f.fileno())

    with open("data_with_headers.csv", "w") as f:
        f.write("c,b,a\n")
        for _ in range(10):
            f.write(data)
        os.fsync(f.fileno())

    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True
    )

    try:
        client.fput_object(
            config["S3_BUCKET"],
            f"{run_id}_data_without_headers.csv",
            f"data_without_headers.csv"

        )
        client.fput_object(
            config["S3_BUCKET"],
            f"{run_id}_data_with_headers.csv",
            f"data_with_headers.csv"
        )
        print(
            f"Uploaded {run_id}_data_with_headers.csv &  {run_id}_data_with_headers.csv to S3")
        os.remove(f"data_with_headers.csv")
        os.remove(f"data_without_headers.csv")
    except Exception as e:
        print(f"Error uploading test files")

    return_code = 0
    try:
        do_test(config, N, n, run_id)
    except Exception as e:
        print("Test failed", e)
        return_code = 1

    # Clean up
    for i in range(20):
        try:
            client.remove_object(
                config["S3_BUCKET"], f"{run_id}_data_with_headers.csv")
            client.remove_object(
                config["S3_BUCKET"], f"{run_id}_data_without_headers.csv")
        except Exception as e:
            print(f"Error removing testing files {e}")

    exit(return_code)
