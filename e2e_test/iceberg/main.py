import traceback
from pyspark.sql import SparkSession
import argparse
import configparser
import subprocess
import csv
import unittest
import time
import os
import tomli as toml
from datetime import date
from datetime import datetime
from datetime import timezone
import decimal
import glob
from typing import List, Dict, Any, Optional
from enum import Enum, auto
import sys


class LogLevel(Enum):
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    HEADER = auto()
    SEPARATOR = auto()
    RESULT = auto()


class Color:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def log(msg, level: LogLevel = LogLevel.INFO, indent: int = 0):
    """Enhanced logging function with color support and structure.

    Args:
        msg: The message to log
        level: LogLevel enum indicating the type of message
        indent: Number of spaces to indent the message
    """
    prefix = " " * indent
    timestamp = datetime.now().strftime("%H:%M:%S")

    if level == LogLevel.DEBUG:
        color = Color.BLUE
        prefix = f"{prefix}[DEBUG] "
    elif level == LogLevel.INFO:
        color = Color.GREEN
        prefix = f"{prefix}[INFO] "
    elif level == LogLevel.WARNING:
        color = Color.YELLOW
        prefix = f"{prefix}[WARN] "
    elif level == LogLevel.ERROR:
        color = Color.RED
        prefix = f"{prefix}[ERROR] "
    elif level == LogLevel.HEADER:
        color = Color.HEADER + Color.BOLD
        prefix = f"{prefix}=== "
        msg = f"{msg} ==="
    elif level == LogLevel.SEPARATOR:
        color = Color.BLUE
        msg = "=" * 40
        prefix = prefix
    elif level == LogLevel.RESULT:
        color = Color.GREEN + Color.BOLD
        prefix = f"{prefix}>>> "
    else:
        color = ""
        prefix = prefix

    # Only use colors if we're writing to a terminal
    if not sys.stdout.isatty():
        color = ""
        Color.ENDC = ""

    print(f"{color}{prefix}{msg}{Color.ENDC}", flush=True)


def strtobool(v):
    return v.lower() == "true"


def strtodate(v):
    return date.fromisoformat(v)


def strtots(v):
    return datetime.fromisoformat(v).astimezone(timezone.utc).replace(tzinfo=None)


g_spark: Optional[SparkSession] = None


def get_spark(args) -> SparkSession:
    spark_config = args["spark"]
    global g_spark
    if g_spark is None:
        g_spark = SparkSession.builder.remote(spark_config["url"]).getOrCreate()

    return g_spark


def init_iceberg_table(args, init_sqls):
    spark = get_spark(args)
    for sql in init_sqls:
        log(f"Executing sql: {sql}", level=LogLevel.INFO)
        spark.sql(sql)


def execute_slt(args, slt):
    if slt is None or slt == "":
        return
    cmd = f"risedev slt {slt}"
    log(f"Command line is [{cmd}]", level=LogLevel.INFO)
    subprocess.run(
        cmd,
        shell=True,
        env={"CARGO_MAKE_PRINT_TIME_SUMMARY": "false"},
        check=True,
    )
    time.sleep(15)


def verify_result(args, verify_sql, verify_schema, verify_data):
    tc = unittest.TestCase()

    time.sleep(3)
    log(f"verify_result:", level=LogLevel.HEADER)
    log(f"Executing sql: {verify_sql}", level=LogLevel.INFO, indent=2)
    spark = get_spark(args)
    df = spark.sql(verify_sql).collect()
    log(f"Result:", level=LogLevel.HEADER, indent=2)
    log("", level=LogLevel.SEPARATOR, indent=2)
    for row in df:
        log(row, level=LogLevel.RESULT, indent=4)
    log("", level=LogLevel.SEPARATOR, indent=2)
    rows = verify_data.splitlines()
    tc.assertEqual(len(df), len(rows), "row length mismatch")
    tc.assertEqual(len(verify_schema), len(df[0]), "column length mismatch")
    for row1, row2 in zip(df, rows):
        log(f"Row1: {row1}, Row 2: {row2}", level=LogLevel.DEBUG, indent=4)
        # New parsing logic for row2
        row2 = parse_row(row2)
        for idx, ty in enumerate(verify_schema):
            if ty == "int" or ty == "long":
                tc.assertEqual(row1[idx], int(row2[idx]))
            elif ty == "float" or ty == "double":
                tc.assertEqual(round(row1[idx], 5), round(float(row2[idx]), 5))
            elif ty == "boolean":
                tc.assertEqual(row1[idx], strtobool(row2[idx]))
            elif ty == "date":
                tc.assertEqual(row1[idx], strtodate(row2[idx]))
            elif ty == "timestamp":
                tc.assertEqual(
                    row1[idx].astimezone(timezone.utc).replace(tzinfo=None),
                    strtots(row2[idx]),
                )
            elif ty == "timestamp_ntz":
                tc.assertEqual(row1[idx], datetime.fromisoformat(row2[idx]))
            elif ty == "string":
                tc.assertEqual(row1[idx], row2[idx])
            elif ty == "decimal":
                if row2[idx] == "none":
                    tc.assertTrue(row1[idx] is None)
                else:
                    tc.assertEqual(row1[idx], decimal.Decimal(row2[idx]))
            else:
                tc.assertEqual(str(row1[idx]), str(row2[idx]))


def compare_sql(args, cmp_sqls):
    assert len(cmp_sqls) == 2
    spark = get_spark(args)
    df1 = spark.sql(cmp_sqls[0])
    df2 = spark.sql(cmp_sqls[1])

    tc = unittest.TestCase()
    diff_df = df1.exceptAll(df2).collect()
    log(f"diff {diff_df}", level=LogLevel.INFO)
    tc.assertEqual(len(diff_df), 0)
    diff_df = df2.exceptAll(df1).collect()
    log(f"diff {diff_df}", level=LogLevel.INFO)
    tc.assertEqual(len(diff_df), 0)


def drop_table(args, drop_sqls):
    spark = get_spark(args)
    for sql in drop_sqls:
        log(f"Executing sql: {sql}", level=LogLevel.INFO)
        spark.sql(sql)


def parse_row(row):
    result = []
    current = ""
    parenthesis_count = {"{": 0, "[": 0, "(": 0}
    for char in row:
        if char in parenthesis_count:
            parenthesis_count[char] += 1
        elif char == "}":
            parenthesis_count["{"] -= 1
        elif char == "]":
            parenthesis_count["["] -= 1
        elif char == ")":
            parenthesis_count["("] -= 1

        if char == "," and all(value == 0 for value in parenthesis_count.values()):
            result.append(current.strip())
            current = ""
        else:
            current += char

    if current:
        result.append(current.strip())

    return result


def discover_test_cases() -> List[str]:
    """Discover all test case files in the test_case directory."""
    test_case_dir = os.path.join(os.path.dirname(__file__), "test_case")
    # Find all .toml files in test_case directory, excluding benches directory
    test_files = glob.glob(os.path.join(test_case_dir, "*.toml"))
    test_files.extend(glob.glob(os.path.join(test_case_dir, "*/*.toml")))
    # Filter out bench files
    test_files = [f for f in test_files if "benches" not in f]
    return test_files


def run_test_case(test_file: str, args: Dict[str, Any]) -> None:
    """Run a single test case."""
    log(f"\nRunning test case: {test_file}", level=LogLevel.HEADER)
    with open(test_file, "rb") as f:
        test_case = toml.load(f)

        # Extract content from testcase
        init_sqls = test_case["init_sqls"]
        log(f"init_sqls:", level=LogLevel.INFO)
        for sql in init_sqls:
            log(sql, level=LogLevel.INFO, indent=2)
        slt = test_case.get("slt")
        log(f"slt: {slt}", level=LogLevel.INFO)
        verify_schema = test_case.get("verify_schema")
        log(f"verify_schema: {verify_schema}", level=LogLevel.INFO)
        verify_sql = test_case.get("verify_sql")
        log(f"verify_sql: {verify_sql}", level=LogLevel.INFO)
        verify_data = test_case.get("verify_data")
        verify_slt = test_case.get("verify_slt")
        cmp_sqls = test_case.get("cmp_sqls")
        drop_sqls = test_case["drop_sqls"]

        try:
            init_iceberg_table(args, init_sqls)
            if slt:
                execute_slt(args, slt)
            if verify_data and verify_sql and verify_schema:
                verify_result(args, verify_sql, verify_schema, verify_data)
            if cmp_sqls and len(cmp_sqls) == 2:
                compare_sql(args, cmp_sqls)
            if verify_slt:
                execute_slt(args, verify_slt)
            if drop_sqls:
                drop_table(args, drop_sqls)
        except Exception as e:
            log(f"test case {test_file} failed: {e}", level=LogLevel.ERROR)
            raise e


def get_parallel_job_info() -> Optional[tuple[int, int]]:
    """Get parallel job information from environment variables."""
    job = os.environ.get("BUILDKITE_PARALLEL_JOB")
    if job is None:
        return None
    job = int(job)
    total = int(os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT"))
    return job, total


def prepare_test_env():
    log("prepare test env", level=LogLevel.HEADER)
    log("create minio bucket", level=LogLevel.INFO)
    subprocess.run(
        ["risedev", "mc", "mb", "-p", "hummock-minio/icebergdata"],
        check=True,
    )
    log("start spark connect server", level=LogLevel.INFO)
    subprocess.run(
        [
            os.path.join(os.path.dirname(__file__), "start_spark_connect_server.sh"),
        ],
        check=True,
    )
    log("prepare test env done", level=LogLevel.HEADER)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test script for iceberg")
    parser.add_argument(
        "-t",
        dest="test_case",
        type=str,
        help="Optional: specific test case file (toml file) to run. If not provided, all test cases will be run.",
    )

    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read("config.ini")
    config_dict = {section: dict(config[section]) for section in config.sections()}

    prepare_test_env()

    if args.test_case:
        # Run single test case if specified
        run_test_case(args.test_case, config_dict)
    else:
        # Run discovered test cases
        test_files = discover_test_cases()
        log(f"Discovered {len(test_files)} test cases", level=LogLevel.INFO)

        # Get parallel job information
        parallel_info = get_parallel_job_info()
        if parallel_info:
            job_index, total_jobs = parallel_info
            # Distribute test files among parallel jobs
            test_files.sort()  # Ensure consistent distribution
            test_files = test_files[job_index::total_jobs]
            log(
                f"Running job {job_index + 1} of {total_jobs} with {len(test_files)} test cases",
                level=LogLevel.INFO,
            )

        # Run all test cases assigned to this job
        for test_file in test_files:
            run_test_case(test_file, config_dict)
