import os
import json
from time import sleep
import psycopg2


def do_test(config):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    # Create table with connector
    cur.execute(f'''CREATE TABLE t (v1 int, v2 varchar)
                    WITH (
                    connector='pulsar',
                    topic='persistent://tenant0/default/topic0',
                    service.url='pulsar+ssl://pulsar-gcp-useast1.streaming.datastax.com:6651',
                    auth.token='{config['ASTRA_STREAMING_TOKEN']}'
                    )
                    ROW FORMAT JSON''')
    sleep(5)

    # Execute SELECT statements
    cur.execute('select count(*) from t')
    result = cur.fetchone()
    print(result[0])
    assert result == (5,)

    expected_data = [1, "name0",
                     2, "name0",
                     6, "name3",
                     0, "name5",
                     5, "name8"]

    cur.execute('select * from t')
    for i in range(5):
        sleep(1)
        result = cur.fetchone()
        print(result)
        assert result[0] == expected_data[i * 2]
        assert result[1] == expected_data[i * 2 + 1]

    # Clean up
    cur.execute('drop table t')

    cur.close()
    conn.close()


if __name__ == "__main__":
    config = json.loads(os.environ["ASTRA_STREAMING_TEST_TOKEN"])

    return_code = 0
    try:
        do_test(config)
    except Exception as e:
        print("Test failed", e)
        return_code = 1

    exit(return_code)
