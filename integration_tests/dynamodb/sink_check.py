import sys
import subprocess
import json

curl = """
curl -X POST http://localhost:8000/ \
            -H "Accept-Encoding: identity" \
            -H "X-Amz-Target: DynamoDB_20120810.Scan" \
            -H "Content-Type: application/x-amz-json-1.0" \
            -H "Authorization: AWS4-HMAC-SHA256 Credential=DUMMY/20240601/us/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=DUMMY" \
            --data '{"TableName": "Movies"}'
"""

output = subprocess.Popen(
    ["bash", "-c", curl,],
    stdout=subprocess.PIPE,
)
msgs = json.loads(output.stdout.read())
print(msgs)

item_count = msgs["Count"]
items = msgs["Items"]
print("item count: ", item_count)
print("items: ", items)

output.stdout.close()
output.wait()

if item_count != 4:
    print("Data check failed")
    sys.exit(1)

for item in items:
    title = item["title"]["S"]
    year = item["year"]["N"]
    description = item["description"]["S"]

    if title not in ["Black Panther", "Avengers: Endgame", "Beautiful beauty", "The Emoji Movie"]:
        print(f"Data check failed: unexcept title: {title}")
        sys.exit(1)

    if int(year) not in range(2017, 2021):
        print(f"Data check failed: unexcept year: {year}")
        sys.exit(1)

    if description not in ["a", "b", "c", "ABC"]:
        print(f"Data check failed: unexcept description: {description}")
        sys.exit(1)
