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
                    topic='persistent://public/default/topic-0',
                    service.url='pulsar+ssl://cluster-0-e6a6f50b-57ef-4db6-94a3-0884b65ab8fb.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651',
                    oauth.issuer.url='https://auth.streamnative.cloud/',
                    oauth.credentials.url='{config['STREAMNATIVE_CLOUD_KEYFILE_URL']}',
                    oauth.audience='urn:sn:pulsar:o-d6jgy:instance-0',
                    access_key='{config['STREAMNATIVE_CLOUD_ACCESS_KEY']}',
                    secret_access='{config['STREAMNATIVE_CLOUD_SECRET_ACCESS']}'
                    )
                    ROW FORMAT JSON''')
    sleep(5)

    # Do test with slt
    ret = os.system(
        "sqllogictest -p 4566 -d dev './e2e_test/source/pulsar/pulsar.slt'")
    # Clean up
    cur.execute('drop table t')

    cur.close()
    conn.close()
    assert ret == 0


if __name__ == "__main__":
    config = json.loads(os.environ["STREAMNATIVE_CLOUD_TEST_CONF"])

    do_test(config)
