import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, check_row_counts, report_failures


def cockroach_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    output = docker_compose_exec("postgres", f'psql -U root -h cockroachdb -p 26257 -d defaultdb --tuples-only -c "{sql}"')
    return int(output.strip())


failed = check_row_counts(['target_count', 'data_types', 'cock_all_data_types'], cockroach_count, "cockroach")
report_failures(failed)
