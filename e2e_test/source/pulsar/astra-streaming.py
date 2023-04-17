import os
import json
from time import sleep
import psycopg2
import re


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

    # Do test with slt
    ret = os.popen(
        "sqllogictest -p 4566 -d dev './e2e_test/source/pulsar/pulsar.slt'").read()
    print("astra-streaming test result: ", ret)
    result = re.match(
        'e2e_test/source/pulsar/pulsar.slt +.. \[OK\] in [0-9]+ ms', ret)

    # Clean up
    cur.execute('drop table t')

    cur.close()
    conn.close()
    assert result != None


if __name__ == "__main__":
    config = json.loads(os.environ["ASTRA_STREAMING_TEST_TOKEN"])

    do_test(config)
