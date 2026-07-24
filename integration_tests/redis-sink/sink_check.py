import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from sink_check_utils import check_row_counts, report_failures


def redis_key_count(rel):
    query = f"*{rel}*"
    print(f"Running query: scan {query} on Redis")
    output = subprocess.Popen(
        ["docker", "compose", "exec", "redis", "redis-cli", "--scan", "--pattern", query],
        stdout=subprocess.PIPE,
    )
    rows = subprocess.check_output(["wc", "-l"], stdin=output.stdout)
    output.stdout.close()
    output.wait()
    return int(rows.decode("utf8").strip())


failed = check_row_counts(['user_id', 'UserID', 'types_id', 'TYPESID'], redis_key_count, "Redis")
report_failures(failed)
