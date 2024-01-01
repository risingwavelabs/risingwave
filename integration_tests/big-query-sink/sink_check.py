import json
import subprocess
import sys

relations = ['rwctest.bqtest.bq_sink']

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

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
