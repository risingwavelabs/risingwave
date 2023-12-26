import subprocess
from time import sleep

sleep(60)

query_sql = open("spark-script/query-table.sql").read()

print("querying deltalake with sql: %s" % query_sql)

query_output_file_name = "query_output.txt"
# Wait docker image version
# query_output_file_name_rust = "query_output_rust.txt"

query_output_file = open(query_output_file_name, "wb")
# query_output_file_rust = open(query_output_file_name_rust, "wb")

subprocess.run(
    ["docker", "compose", "exec", "spark", "bash", "/spark-script/run-sql-file.sh", "query-table"],
    check=True, stdout=query_output_file)
query_output_file.close()

# subprocess.run(
#     ["docker", "compose", "exec", "spark", "bash", "/spark-script/run-sql-file.sh", "query-table"],
#     check=True, stdout=query_output_file_rust)
# query_output_file_rust.close()

output_content = open(query_output_file_name).read()
# output_content_rust = open(query_output_file_name_rust).read()

print(output_content)
# print(output_content_rust)

assert len(output_content.strip()) > 1
# assert len(output_content_rust.strip()) > 1
