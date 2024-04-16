import subprocess
from time import sleep

sleep(60)

query_sql = open("iceberg-query.sql").read()

print("querying iceberg with presto sql: %s" % query_sql)

query_output_file_name = "query_output.txt"

query_output_file = open(query_output_file_name, "wb")

subprocess.run(
    ["docker", "compose", "exec", "presto", "presto-cli", "--server", "localhost:8080", "--execute", query_sql],
    check=True, stdout=query_output_file)
query_output_file.close()

output_content = open(query_output_file_name).read()

print(output_content)

assert len(output_content.strip()) > 0
