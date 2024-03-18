import sys
import subprocess


output = subprocess.Popen(["docker", "compose", "exec", "mqtt-server", "mosquitto_sub", "-h", "localhost", "-t", "test", "-p", "1883", "-C",  "1", "-W", "120"],
                            stdout=subprocess.PIPE)
rows = subprocess.check_output(["wc", "-l"], stdin=output.stdout)
output.stdout.close()
output.wait()
rows = int(rows.decode('utf8').strip())
print(f"{rows} rows in 'test'")
if rows < 1:
    print(f"Data check failed for case 'test'")
    sys.exit(1)
