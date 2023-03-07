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

DEFAULT_S3_ENDPOINT  = "s3.amazonaws.com"
S3_ENDPOINT = os.environ.get("S3_ENDPOINT",DEFAULT_S3_ENDPOINT)
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY","")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY","")
S3_BUCKET = os.environ.get("S3_BUCKET","s3-test-ci")
S3_REGION = os.environ.get("S3_REGION","us-east-1")

print(f"{S3_ENDPOINT} AK:${len(S3_ACCESS_KEY)} SK:${len(S3_SECRET_KEY)} ${S3_BUCKET} ${S3_REGION}")
if S3_ENDPOINT == DEFAULT_S3_ENDPOINT:
    client = Minio(
        S3_ENDPOINT,
        secure=True
    )
else:
    client = Minio(
        S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        secure=True
    )


for i in range(20):
    try:
        client.fput_object(
            S3_BUCKET,
            f"data_{i}.ndjson",
            f"data_{i}.ndjson"
        )
        print(f"Uploaded data_{i}.ndjson to S3")
        os.remove(f"data_{i}.ndjson")
    except Exception as e:
        print(f"Error uploading data_{i}.ndjson: {e}")

if S3_ENDPOINT == DEFAULT_S3_ENDPOINT:
    with open("./e2e_test/s3/s3.aws.slt.tlp", "r") as f:
        template_str = f.read()


    template = string.Template(template_str)

    output_str = template.substitute(
        S3_BUCKET=S3_BUCKET,
        S3_REGION=S3_REGION,
        COUNT=int(N*n),
        SUM_ID=int(((N - 1) * N / 2) * n),
        SUM_SEX=int(N*n / 2),
        SUM_MARK=0
    )
else:
    pass
    # todo

# Output the resulting string to file
with open("./e2e_test/s3/s3.slt", "w") as f:
    f.write(output_str)


result = subprocess.run(
    "sqllogictest -p 4566 -d dev './e2e_test/s3/s3.slt'", shell=True)

# Clean up
for i in range(20):
    try:
        client.remove_object(S3_BUCKET, f"data_{i}.ndjson")
        print(f"Removed data_{i}.ndjson from S3")
    except Exception as e:
        print(f"Error removing data_{i}.ndjson: {e}")

exit(result.returncode)
