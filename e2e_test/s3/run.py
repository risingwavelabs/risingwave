import os
import string
import json
import string
from time import sleep
from minio import Minio
import psycopg2


def do_test(config, N, n):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE s3_test( 
        id int,
        name TEXT,
        sex int,
        mark int,
    ) WITH (
        connector = 's3',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
    ) ROW FORMAT json;''')

    total_row = int(N * n)
    while True:
        sleep(10)
        cur.execute('select count(*) from s3_test')
        result = cur.fetchone()
        if result[0] == total_row:
            break
        print(
            f"Now got {result[0]} rows in table, {total_row} expected, wait 10s")

    cur.execute('select count(*), sum(id), sum(sex), sum(mark) from s3_test')
    result = cur.fetchone()

    print(result)

    assert result[0] == total_row
    assert result[1] == int(((N - 1) * N / 2) * n)
    assert result[2] == int(N*n / 2)
    assert result[3] == 0

    cur.execute('drop table s3_test')

    cur.close()
    conn.close()


if __name__ == "__main__":
    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])

    N = 10000

    items = [
        {
            "id": j,
            "name": str(j),
            "sex": j % 2,
            "mark": -1 if j % 2 else 1,
        }
        for j in range(N)
    ]

    data = "\n".join([json.dumps(item) for item in items]) + "\n"
    n = 0
    with open("data_0.ndjson", "w") as f:
        for _ in range(1000):
            n += 1
            f.write(data)
        os.fsync(f.fileno())

    for i in range(1, 20):
        with open(f"data_{i}.ndjson", "w") as f:
            n += 1
            f.write(data)
            os.fsync(f.fileno())

    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True
    )

    for i in range(20):
        try:
            client.fput_object(
                config["S3_BUCKET"],
                f"data_{i}.ndjson",
                f"data_{i}.ndjson"
            )
            print(f"Uploaded data_{i}.ndjson to S3")
            os.remove(f"data_{i}.ndjson")
        except Exception as e:
            print(f"Error uploading data_{i}.ndjson: {e}")

    return_code = 0
    try:
        do_test(config, N, n)
    except Exception as e:
        print("Test failed", e)
        return_code = 1

    # Clean up
    for i in range(20):
        try:
            client.remove_object(config["S3_BUCKET"], f"data_{i}.ndjson")
            print(f"Removed data_{i}.ndjson from S3")
        except Exception as e:
            print(f"Error removing data_{i}.ndjson: {e}")

    exit(return_code)
