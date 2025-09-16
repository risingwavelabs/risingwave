import sys
import subprocess


relations = ['target_count', 'data_types', 'pg_all_data_types', 'numeric256_types']

failed_cases = []
for rel in relations:
    sql = f'SELECT COUNT(*) FROM {rel};'
    print(f"Running SQL: {sql} ON PG")
    command = f'psql -U $POSTGRES_USER $POSTGRES_DB --tuples-only -c "{sql}"'
    rows = subprocess.check_output(
        ["docker", "compose", "exec", "postgres", "bash", "-c", command])
    rows = int(rows.decode('utf-8').strip())
    print(f"{rows} rows in {rel}")
    # Special case for numeric256_types which should have exactly 7 rows
    if rel == 'numeric256_types' and rows != 7:
        failed_cases.append(f"{rel} (expected 7 rows, got {rows})")
    elif rel != 'numeric256_types' and rows < 1:
        failed_cases.append(rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
