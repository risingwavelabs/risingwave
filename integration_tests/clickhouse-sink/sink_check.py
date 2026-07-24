import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, check_row_counts, report_failures


def clickhouse_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    output = docker_compose_exec("clickhouse-server", f'clickhouse-client -q "{sql}"')
    return int(output.strip())


failed = check_row_counts(
    ['default.demo_test', 'default.demo_test_target_null', 'default.ck_types'],
    clickhouse_count,
    "ClickHouse",
)
report_failures(failed)
