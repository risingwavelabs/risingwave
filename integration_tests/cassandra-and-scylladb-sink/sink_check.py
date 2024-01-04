import subprocess
import sys
from time import sleep

sleep(30)

relations = ['demo.demo_bhv_table']

dbs = ['cassandra', 'scylladb']
failed_cases = []
for rel in relations:
    sql = f'select count(*) from {rel};'
    for db in dbs:
        print(f"Running SQL: {sql} on {db}")
        query_output_file_name = f"query_{db}_output.txt"
        query_output_file = open(query_output_file_name, "wb+")

        subprocess.run(["docker", "compose", "exec", db, "cqlsh", "-e", sql], check=True,
                       stdout=query_output_file)

        # output file:
        #
        #  count
        # -------
        #   1000
        #
        # (1 rows)
        query_output_file.seek(0)
        lines = query_output_file.readlines()
        query_output_file.close()
        assert len(lines) >= 6
        assert lines[1].decode('utf-8').strip().lower() == 'count'
        rows = int(lines[3].decode('utf-8').strip())
        print(f"{rows} rows in {db}.{rel}")
        if rows < 1:
            failed_cases.append(db + "_" + rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
