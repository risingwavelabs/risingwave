import subprocess
from time import sleep

sleep(60)

query_sql = open("spark-script/query-table.sql").read()

print("querying deltalake with sql: %s" % query_sql)

query_output_file_name = "query_output.txt"

query_output_file = open(query_output_file_name, "wb")

subprocess.run(
    ["docker", "compose", "exec", "spark", "bash", "/spark-script/run-sql-file.sh", "query-table"],
    check=True, stdout=query_output_file)
query_output_file.close()

with open(query_output_file_name, 'r') as file:
    all_lines = file.readlines()

last_three_lines = all_lines[-3:]

print("result", last_three_lines)

line1, line2, line3 = last_three_lines

assert line1.strip() == '1\ta'
assert line2.strip() == '2\tb'
assert line3.strip() == '3\tc'