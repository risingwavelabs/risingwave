import sys
import subprocess


output = subprocess.Popen(
    [
        "docker",
        "compose",
        "exec",
        "mqtt",
        "mosquitto_sub",
        "-h",
        "hivemq-ce",
        "-t",
        "test",
        "-p",
        "1883",
        "-C",
        "1",
        "-W",
        "120",
    ],
    stdout=subprocess.PIPE,
)

rows = subprocess.check_output(["wc", "-l"], stdin=output.stdout)
output.stdout.close()  # type: ignore
output.wait()

rows = int(rows.decode("utf8").strip())

print(f"{rows} rows in 'test'")
if rows < 1:
    print("Data check failed for case 'test'")
    sys.exit(1)
