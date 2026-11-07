import sys
from pathlib import Path

from util import DATASET, bq_query, drop_bq_table

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import report_failures


def append_only_checks():
    relations = [f"{DATASET}.bq_sink", f"{DATASET}.bq_sink_data_types"]
    failed_cases = []
    for rel in relations:
        sql = f"SELECT COUNT(*) AS count FROM `{rel}`"
        print(f"run sql: {sql} on Bigquery")
        rows = int(bq_query(sql)[0]["count"])
        print(f"{rows} rows in {rel}")
        if rows < 1:
            failed_cases.append(rel)

        drop_bq_table(rel)

    report_failures(failed_cases)


if __name__ == "__main__":
    append_only_checks()
