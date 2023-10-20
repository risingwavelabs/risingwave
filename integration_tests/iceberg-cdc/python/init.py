from pyspark.sql import SparkSession
import configparser
import psycopg2


def init_spark_table(args):
    spark_config = args['spark']
    spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()

    init_table_sqls = [
        "CREATE SCHEMA IF NOT EXISTS s1",
        "DROP TABLE IF EXISTS s1.t1",
        """
        CREATE TABLE s1.t1
        (
            user_id string,
            target_id string,
            target_type string,
            event_timestamp string,
            behavior_type string,
            parent_target_type string,
            parent_target_id string
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');
        """,
    ]

    for sql in init_table_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def init_risingwave_mv(args):
    rw_config = args['risingwave']
    sqls = [
        "set streaming_parallelism = 4",
        """
        CREATE TABLE user_behaviors (
            user_id VARCHAR,
            target_id VARCHAR,
            target_type VARCHAR,
            event_timestamp VARCHAR,
            behavior_type VARCHAR,
            parent_target_type VARCHAR,
            parent_target_id VARCHAR,
            PRIMARY KEY(user_id, target_id, event_timestamp)
        ) with (
            connector = 'mysql-cdc',
            hostname = 'mysql',
            port = '3306',
            username = 'root',
            password = '123456',
            database.name = 'mydb',
            table.name = 'user_behaviors',
            server.id = '1'
        );
        """,
        # f"""
        # CREATE SINK s1
        # AS SELECT * FROM user_behaviors
        # WITH (
        #     connector='iceberg',
        #     type='upsert',
        #     primary_key = 'user_id, target_id, event_timestamp',
        #     catalog.type = 'storage',
        #     s3.endpoint = 'http://minio-0:9301',
        #     s3.access.key = 'hummockadmin',
        #     s3.secret.key = 'hummockadmin',
        #     database.name='demo',
        #     table.name='s1.t1',warehouse.path = 's3://hummock001/icebergdata/demo',s3.region = 'us-east-1'
        # );
        # """
        f"""
        CREATE SINK s1
        AS SELECT * FROM user_behaviors
        WITH (
            connector='iceberg',
            type='upsert',
            primary_key = 'user_id, target_id, event_timestamp',
            catalog.type = 'rest',
            catalog.uri = 'http://rest:8181',
            s3.endpoint = 'http://minio-0:9301',
            s3.access.key = 'hummockadmin',
            s3.secret.key = 'hummockadmin',
            database.name='demo',
            table.name='s1.t1',warehouse.path = 's3://icebergdata/demo/s1/t1',s3.region = 'us-east-1'
        );
        """
    ]
    with psycopg2.connect(database=rw_config['db'], user=rw_config['user'], host=rw_config['host'],
                          port=rw_config['port']) as conn:
        with conn.cursor() as cursor:
            for sql in sqls:
                print(f"Executing sql {sql}")
                cursor.execute(sql)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    init_spark_table(config)
    init_risingwave_mv(config)
