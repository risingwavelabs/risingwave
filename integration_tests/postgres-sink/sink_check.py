import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, check_row_counts, report_failures


def pg_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    output = docker_compose_exec("postgres", f'psql -U $POSTGRES_USER $POSTGRES_DB --tuples-only -c "{sql}"')
    return int(output.strip())


failed = check_row_counts(['target_count', 'data_types', 'pg_all_data_types'], pg_count, "PG")
report_failures(failed)
