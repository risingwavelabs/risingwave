import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, check_row_counts, report_failures


def tidb_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    output = docker_compose_exec("mysql", f'mysql --password= -h tidb --port 4000 -u root test -e "{sql}"')
    return int(output.split('\n')[1])


failed = check_row_counts(['hot_hashtags', 'tidb_sink_datatypes'], tidb_count, "TiDB")
report_failures(failed)
