import os
import string
import json
import string
from minio import Minio
import subprocess
N = 10000

items = [
    {
        "id": j,
        "name": str(j),
        "sex": j % 2,
        "mark": -1 if j % 2 else 1,
    }
    for j in range(N)
]

data = "\n".join([json.dumps(item) for item in items]) + "\n"
n = 0
with open("data_0.ndjson", "w") as f:
    for _ in range(1000):
        n += 1
        f.write(data)
    os.fsync(f.fileno())

for i in range(1, 20):
    with open(f"data_{i}.ndjson", "w") as f:
        n += 1
        f.write(data)
        os.fsync(f.fileno())


config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])

client = Minio(
    config["S3_ENDPOINT"],
    access_key=config["S3_ACCESS_KEY"],
    secret_key=config["S3_SECRET_KEY"],
    secure=True
)


for i in range(20):
    try:
        client.fput_object(
            config["S3_BUCKET"],
            f"data_{i}.ndjson",
            f"data_{i}.ndjson"
        )
        print(f"Uploaded data_{i}.ndjson to S3")
        os.remove(f"data_{i}.ndjson")
    except Exception as e:
        print(f"Error uploading data_{i}.ndjson: {e}")

with open("./e2e_test/s3/s3.slt.tlp", "r") as f:
    template_str = f.read()


template = string.Template(template_str)

output_str = template.substitute(
    **config,
    S3_ENDPOINT="https://" + config["S3_ENDPOINT"],
    COUNT=int(N*n),
    SUM_ID=int(((N - 1) * N / 2) * n),
    SUM_SEX=int(N*n / 2),
    SUM_MARK=0,
)
# Output the resulting string to file
with open("./e2e_test/s3/s3.slt", "w") as f:
    f.write(output_str)


result = subprocess.run(
    "sqllogictest -p 4566 -d dev './e2e_test/s3/s3.slt'", shell=True)

# Clean up
for i in range(20):
    try:
        client.remove_object(config["S3_BUCKET"], f"data_{i}.ndjson")
        print(f"Removed data_{i}.ndjson from S3")
    except Exception as e:
        print(f"Error removing data_{i}.ndjson: {e}")

exit(result.returncode)
