import json
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import report_failures

relations = ['rwctest.bqtest.bq_sink', 'rwctest.bqtest.bq_sink_data_types']

failed_cases = []
for rel in relations:
    sql = f"SELECT COUNT(*) AS count FROM `{rel}`"
    print(f"run sql: {sql} on Bigquery")
    rows = subprocess.check_output(
        ["docker", "compose", "exec", "gcloud-cli", "bq", "query", "--use_legacy_sql=false", "--format=json", sql],
    )
    rows = int(json.loads(rows.decode("utf-8").strip())[0]['count'])
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

    drop_sql = f"DROP TABLE IF EXISTS `{rel}`"
    subprocess.run(["docker", "compose", "exec", "gcloud-cli", "bq", "query", "--use_legacy_sql=false", drop_sql],
                   check=True)

report_failures(failed_cases)
