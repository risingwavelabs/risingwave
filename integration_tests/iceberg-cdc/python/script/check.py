from pyspark.sql import SparkSession
import configparser
import psycopg2

def check_spark_table(args):
    expect_row_count = 0
    rw_config = args['risingwave']
    with psycopg2.connect(database=rw_config['db'], user=rw_config['user'], host=rw_config['host'],
                          port=rw_config['port']) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM user_behaviors")
            expect_row_count = cursor.fetchone()[0]
    print(f"expect_row_count is {expect_row_count}")
    spark_config = args['spark']
    spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()
    actual_row_count = spark.sql("SELECT COUNT(*) FROM s1.t1").collect()[0][0]
    print(f"actual_row_count is {actual_row_count}")
    assert actual_row_count==expect_row_count


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    check_spark_table(config)
