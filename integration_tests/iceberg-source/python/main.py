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


def init_risingwave_source(docker):
    config = read_config(f"{docker.case_dir()}/config.ini")

    source_config = config['source']
    source_param = ",\n".join([f"{k}='{v}'" for k, v in source_config.items()])

    sqls = [
        f"""
        CREATE SOURCE iceberg_source
        WITH (
            {source_param}
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

def check_risingwave_iceberg_source(docker):
    config = read_config(f"{docker.case_dir()}/config.ini")

    sqls = [
        "select count(*) from iceberg_source",
        "select count(*) from iceberg_source for system_time as of '2100-01-01 00:00:00+00:00'",
        "select count(*) from iceberg_source for system_time as of 4102444800"
    ]

    rw_config = config['risingwave']
    with psycopg2.connect(database=rw_config['db'], user=rw_config['user'], host=rw_config['host'],
                          port=rw_config['port']) as conn:
        with conn.cursor() as cursor:
            for sql in sqls:
                print(f"Executing sql {sql}")
                # execute sql and collect result
                cursor.execute(sql)
                result = cursor.fetchall()
                assert result[0][0] == 1, f"Inserted result is unexpected: {result[0][0]}, test failed"


def run_case(case):
    with DockerCompose(case) as docker:
        init_spark_table(docker)
        init_risingwave_source(docker)
        print("Let risingwave to run")
        time.sleep(5)
        check_risingwave_iceberg_source(docker)


if __name__ == "__main__":
    # Hive case is temporarily skipped due unstable metastore bootstrap in CI.
    case_names = ["jdbc", "rest", "storage"]
    for case_name in case_names:
        print(f"Running test case: {case_name}")
        run_case(case_name)
