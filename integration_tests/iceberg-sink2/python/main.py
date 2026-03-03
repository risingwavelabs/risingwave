import argparse
import subprocess
from pyspark.sql import SparkSession
import configparser
import psycopg2
import time


def read_config(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    print({section: dict(config[section]) for section in config.sections()})
    return config


class DockerCompose(object):
    def __init__(self, case_name: str):
        self.case_name = case_name

    def case_dir(self):
        return f"../docker/{self.case_name}"

    def get_ip(self, container_name):
        return subprocess.check_output([
            "docker", "inspect", "-f", "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container_name], cwd=self.case_dir()).decode("utf-8").rstrip()

    def __enter__(self):
        subprocess.run(["docker-compose", "up", "-d", "--wait"], cwd=self.case_dir(), check=False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        subprocess.run(["docker", "compose", "down", "-v", "--remove-orphans"], cwd=self.case_dir(),
                       capture_output=True,
                       check=True)


def init_spark_table(docker):
    spark_ip = docker.get_ip(f"{docker.case_name}-spark-1")
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


def init_risingwave_mv(docker):
    config = read_config(f"{docker.case_dir()}/config.ini")
    sink_config = config['sink']
    sink_param = ",\n".join([f"{k}='{v}'" for k, v in sink_config.items()])
    sqls = [
        "set sink_decouple = false",
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


def check_spark_table(docker):
    spark_ip = docker.get_ip(f"{docker.case_name}-spark-1")
    url = f"sc://{spark_ip}:15002"
    print(f"Spark url is {url}")
    spark = SparkSession.builder.remote(url).getOrCreate()

    sqls = [
        "SELECT COUNT(*) FROM s1.t1"
    ]

    for sql in sqls:
        print(f"Executing sql: {sql}")
        result = spark.sql(sql).collect()
        assert result[0][0] > 100, f"Inserted result is too small: {result[0][0]}, test failed"

def run_case(case):
    with DockerCompose(case) as docker:
        init_spark_table(docker)
        init_risingwave_mv(docker)
        print("Let risingwave to run")
        time.sleep(5)
        check_spark_table(docker)


if __name__ == "__main__":
    # Hive case is temporarily skipped due unstable metastore bootstrap in CI.
    case_names = ["rest", "storage", "jdbc"]
    for case_name in case_names:
        print(f"Running test case: {case_name}")
        run_case(case_name)
