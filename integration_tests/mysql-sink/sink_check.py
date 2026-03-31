import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import docker_compose_exec, check_row_counts, report_failures


def mysql_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    output = docker_compose_exec("mysql", f'mysql -p123456 mydb -e "{sql}"')
    return int(output.split('\n')[1])


failed = check_row_counts(['target_count', 'data_types', 'mysql_all_types'], mysql_count, "MySQL")
report_failures(failed)
