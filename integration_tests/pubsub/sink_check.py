import sys
import subprocess
import json

curl = """
PUBSUB_EMULATOR_HOST=localhost:8900 curl -s -X POST 'http://localhost:8900/v1/projects/demo/subscriptions/sub:pull' \
    -H 'content-type: application/json' \
    --data '{"maxMessages":10}'
"""

output = subprocess.Popen(
    ["bash", "-c", curl,],
    stdout=subprocess.PIPE,
)
msgs = json.loads(output.stdout.read())
print(msgs)

msg_count = len(msgs["receivedMessages"])
print("msg count", msg_count)

output.stdout.close()
output.wait()

if msg_count != 8:
    print("Data check failed")
    sys.exit(1)
