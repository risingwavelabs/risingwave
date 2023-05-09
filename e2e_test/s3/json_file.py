import os
import string
import json
import string
from time import sleep
from minio import Minio
import psycopg2
import random


def do_test(client, config, N,  prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE s3_test_jsonfile( 
        id int,
        name TEXT,
        sex int,
        mark int,
    ) WITH (
        connector = 's3',
        match_pattern = '{prefix}*.json',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
    ) ROW FORMAT json;''')

    for i in range(N):
        try:
            client.fput_object(
                config["S3_BUCKET"],
                f"{run_id}_data_{i}.json",
                f"data_{i}.json"
            )
            print(f"Uploaded {run_id}_data_{i}.json to S3")
            os.remove(f"data_{i}.json")
        except Exception as e:
            print(f"Error uploading data_{i}.json: {e}")

    total_row = int(N)
    while True:
        sleep(60)
        cur.execute('select count(*) from s3_test_jsonfile')
        result = cur.fetchone()
        if result[0] == total_row:
            break
        print(
            f"Now got {result[0]} rows in table, {total_row} expected, wait 60s")

    cur.execute(
        'select count(*), sum(id), sum(sex), sum(mark) from s3_test_jsonfile')
    result = cur.fetchone()

    print(result)

    assert result[0] == total_row
    assert result[1] == int(((N - 1) * N / 2))
    assert result[2] == int(N / 2)
    assert result[3] == 0

    cur.execute('drop table s3_test_jsonfile')

    cur.close()
    conn.close()


if __name__ == "__main__":
    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    run_id = str(random.randint(1000, 9999))
    N = 100

    for i in range(N):
        with open(f"data_{i}.json", "w") as f:
            f.write(json.dumps(
                {
                    "id": i,
                    "name": str(i),
                    "sex": i % 2,
                    "mark": -1 if i % 2 else 1,
                }
            ))
            os.fsync(f.fileno())

    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True
    )
    return_code = 0
    try:
        do_test(client, config, N,  run_id)
    except Exception as e:
        print("Test failed", e)
        return_code = 1

    # Clean up
    for i in range(N):
        try:
            client.remove_object(
                config["S3_BUCKET"], f"{run_id}_data_{i}.json")
            print(f"Removed {run_id}_data_{i}.json from S3")
        except Exception as e:
            print(f"Error removing data_{i}.json: {e}")

    exit(return_code)
