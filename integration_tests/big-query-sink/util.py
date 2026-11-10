"""Shared BigQuery CLI helpers for sink integration checks."""

import json
import subprocess
from pathlib import Path


DEMO_DIR = Path(__file__).resolve().parent
DATASET = "rwctest.bqtest"


def require_success(result):
    if result.returncode:
        raise RuntimeError(
            f"Command failed: {result.args}\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout


def bq(*args, timeout=120):
    return require_success(
        subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "gcloud-cli",
                "bq",
                *args,
            ],
            cwd=DEMO_DIR,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    )


def bq_execute(sql, timeout=120):
    return bq(
        "query",
        "--use_legacy_sql=false",
        "--use_cache=false",
        "--format=json",
        sql,
        timeout=timeout,
    )


def bq_query(sql, timeout=120):
    return json.loads(bq_execute(sql, timeout=timeout))


def drop_bq_table(target):
    bq_execute(f"DROP TABLE IF EXISTS `{target}`")
