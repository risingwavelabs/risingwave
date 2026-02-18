from pyspark.sql import SparkSession
import configparser
import psycopg2
import time

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

    # Try to avoid stale snapshot/cache reads; refresh and retry a few times.
    actual_row_count = 0
    for i in range(6):  # total wait up to ~60s
        try:
            # best-effort refresh; ignore if catalog doesn't support it
            spark.sql("REFRESH TABLE s1.t1")
        except Exception:
            pass
        actual_row_count = spark.sql("SELECT COUNT(*) FROM s1.t1").collect()[0][0]
        print(f"actual_row_count is {actual_row_count}")
        if actual_row_count == expect_row_count:
            return
        time.sleep(10)

    raise AssertionError(f"row_count mismatch: actual={actual_row_count}, expect={expect_row_count}")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    check_spark_table(config)
