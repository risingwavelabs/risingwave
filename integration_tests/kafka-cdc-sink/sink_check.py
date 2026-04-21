import subprocess
import sys
from pathlib import Path
from time import sleep
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import check_row_counts, report_failures

# wait for one minute for the flink test pipeline ingestion
print("wait for one minute for ingestion")
sleep(60)


def pg_count(rel):
    sql = f'SELECT COUNT(*) FROM {rel};'
    command = f'psql -U $POSTGRES_USER $POSTGRES_DB --tuples-only -c "{sql}"'
    output = subprocess.check_output(["docker", "exec", "postgres", "bash", "-c", command])
    return int(output.decode('utf8').strip())


failed = check_row_counts(['counts', 'flinkcounts', 'types', 'flink_types'], pg_count, "PG")
report_failures(failed)
