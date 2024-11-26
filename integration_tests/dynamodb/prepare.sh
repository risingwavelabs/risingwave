SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

curl -X POST http://localhost:8000/ \
            -H "Accept-Encoding: identity" \
            -H "X-Amz-Target: DynamoDB_20120810.CreateTable" \
            -H "Content-Type: application/x-amz-json-1.0" \
            -H "Authorization: AWS4-HMAC-SHA256 Credential=DUMMY/20240601/us/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=DUMMY" \
            --data "@$SCRIPT_DIR/create_table_request.json"
