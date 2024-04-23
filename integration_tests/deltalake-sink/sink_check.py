import subprocess
from time import sleep

def run_query(file):
    with open(query_output_file_name, 'w') as f:
        subprocess.run(
            ["docker", "compose", "exec", "spark", "bash", "/spark-script/run-sql-file.sh", file],
            check=True, stdout=f)



query_sql = open("spark-script/query-table.sql").read()

print("querying deltalake with sql: %s" % query_sql)

query_output_file_name = "query_output.txt"

run_query('query-table')
with open(query_output_file_name, 'r') as file:
    all_lines = file.readlines()
    last_three_lines = all_lines[-3:]

    print("result", last_three_lines)
    line1, line2, line3 = last_three_lines
    assert line1.strip() == '1\ta'
    assert line2.strip() == '2\tb'
    assert line3.strip() == '3\tc'


run_query('data-types-query')
with open(query_output_file_name, 'r') as f:
    all_lines = f.readlines()
    last_line = all_lines[-1]
    assert last_line.strip() == '3'
