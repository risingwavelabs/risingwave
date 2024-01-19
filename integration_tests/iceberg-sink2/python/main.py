import argparse
import subprocess
from pyspark.sql import SparkSession
import configparser
import psycopg2
import time


def case_dir(case_name):
    return f"../docker/{case_name}"


def start_docker(case_name):
    subprocess.run(["docker-compose", "up", "-d", "--wait"], cwd=case_dir(case_name), check=False)


def stop_docker(case_name):
    subprocess.run(["docker", "compose", "down", "-v", "--remove-orphans"], cwd=case_dir(case_name),
                   capture_output=True,
                   check=True)


def get_ip(case_name, container_name):
    return subprocess.check_output(["docker", "inspect", "-f", "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{"
                                                               "end}}",
                                    container_name], cwd=case_dir(case_name)).decode("utf-8").rstrip()


def init_spark_table(case_name):
    spark_ip = get_ip(case_dir(case_name), f"{case_name}-spark-1")
    url = f"sc://{spark_ip}:15002"
    print(f"Spark url is {url}")
    spark = SparkSession.builder.remote(url).getOrCreate()

    init_table_sqls = [
        "CREATE SCHEMA IF NOT EXISTS s1",
        "DROP TABLE IF EXISTS s1.t1",
        """
        CREATE TABLE s1.t1
        (
          id bigint,
          name string,
          distance bigint
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');
        """,
        """INSERT INTO s1.t1 VALUES (1, 'test', 100);"""
    ]

    for sql in init_table_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def init_risingwave_mv(config):
    aws_key = config['default']['aws_key']
    aws_secret = config['default']['aws_secret']

    sink_config = config['sink']
    sink_param = ",\n".join([f"{k}='{v}'" for k, v in sink_config.items()])
    sqls = [
        "set streaming_parallelism = 4",
        """
        CREATE SOURCE bid (
            "id" BIGINT,
            "name" VARCHAR,
            "distance" BIGINT,
        ) with (
            connector = 'datagen',
            datagen.split.num = '4',


            fields.id.kind = 'random',
            fields.id.min = '0',
            fields.id.max = '1000000',
            fields.id.seed = '100',

            fields.name.kind = 'random',
            fields.name.length = '15',
            fields.name.seed = '100',

            fields.distance.kind = 'random',
            fields.distance.min = '0',
            fields.distance.max = '100000',
            fields.distance.seed = '100',

            datagen.rows.per.second = '500'
        ) FORMAT PLAIN ENCODE JSON;
        """,
        f"""
        CREATE SINK s1
        AS SELECT * FROM bid
        WITH (
            {sink_param}
            );
        """
    ]

    rw_config = config['risingwave']
    with psycopg2.connect(database=rw_config['db'], user=rw_config['user'], host=rw_config['host'],
                          port=rw_config['port']) as conn:
        with conn.cursor() as cursor:
            for sql in sqls:
                print(f"Executing sql {sql}")
                cursor.execute(sql)


def check_spark_table(case_name):
    spark_ip = get_ip(case_dir(case_name), f"{case_name}-spark-1")
    url = f"sc://{spark_ip}:15002"
    print(f"Spark url is {url}")
    spark = SparkSession.builder.remote(url).getOrCreate()

    sqls = [
        "SELECT COUNT(*) FROM s1.t1"
    ]

    for sql in sqls:
        print(f"Executing sql: {sql}")
        result = spark.sql(sql).collect()
        print(f"Result is {result}")


if __name__ == "__main__":
    case_name = "rest"
    config = configparser.ConfigParser()
    config.read(f"{case_dir(case_name)}/config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    start_docker(case_name)
    print("Waiting for docker to be ready")
    init_spark_table(case_name)
    init_risingwave_mv(config)
    print("Let risingwave to run")
    time.sleep(3)
    check_spark_table(case_name)
    stop_docker(case_name)
