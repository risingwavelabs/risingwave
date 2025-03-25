import decimal
import glob
import os
import subprocess
import time
import unittest
from datetime import date, datetime, timezone
from typing import List

from .log import log, LogLevel
from .spark_utils import get_spark


def strtobool(v):
    return v.lower() == "true"


def strtodate(v):
    return date.fromisoformat(v)


def strtots(v):
    return datetime.fromisoformat(v).astimezone(timezone.utc).replace(tzinfo=None)


def execute_slt(args, slt):
    if slt is None or slt == "":
        return
    rw_config = args["risingwave"]
    cmd = f"sqllogictest -p {rw_config['port']} -d {rw_config['db']} {slt}"
    log(f"Executing slt: {slt}", level=LogLevel.INFO)
    subprocess.run(cmd, shell=True, check=True)
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
    test_case_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "test_case"
    )
    # Find all .toml files in test_case directory, excluding benches directory
    test_files = glob.glob(os.path.join(test_case_dir, "*.toml"))
    test_files.extend(glob.glob(os.path.join(test_case_dir, "*/*.toml")))
    # Filter out bench files
    # Hack: filter out CDC tests, as it uses `--host=mysql` (for CI).
    # TODO: use `risedev slt` to replace `sqllogictest` so that the test can be run locally.
    test_files = [f for f in test_files if "benches" not in f and "cdc" not in f]
    return test_files


def prepare_test_env():
    log("prepare test env", level=LogLevel.HEADER)
    log("create minio bucket", level=LogLevel.INFO)
    subprocess.run(
        [
            "risedev",
            "mc",
            "mb",
            "-p",
            "hummock-minio/icebergdata",
        ],
        check=True,
    )
    log("start spark connect server", level=LogLevel.INFO)
    subprocess.run(
        [
            os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "start_spark_connect_server.sh",
            ),
        ],
        check=True,
    )
    time.sleep(3)
    log("prepare test env done", level=LogLevel.HEADER)
