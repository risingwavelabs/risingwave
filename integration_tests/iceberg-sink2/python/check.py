from pyspark.sql import SparkSession
import configparser


def check_spark_table(args):
    spark_config = args['spark']
    spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()

    sqls = [
        "SELECT COUNT(*) FROM s1.t1"
    ]

    for sql in sqls:
        print(f"Executing sql: {sql}")
        result = spark.sql(sql).collect()
        print(f"Result is {result}")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    check_spark_table(config)
