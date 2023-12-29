import subprocess
import sys
from time import sleep


def check_go():
    print("--- go client test")
    subprocess.run(["docker", "compose", "exec", "go-lang", "bash", "-c", "cd /go-client && ./run.sh"], check=True)


def check_python():
    print("--- python client test")
    subprocess.run(["docker", "compose", "exec", "python", "bash", "-c",
                    "cd /python-client && pip3 install -r requirements.txt && pytest"], check=True)


def check_java():
    print("--- java client test")
    subprocess.run(["docker", "compose", "exec", "java", "bash", "-c", "apt-get update && apt-get install -y maven"],
                   check=True)
    subprocess.run(["docker", "compose", "exec", "java", "bash", "-c", "cd /java-client && mvn clean test"], check=True)


subprocess.run(["docker", "compose", "up", "-d"], check=True)
sleep(10)

failed_cases = []

# try:
#     check_go()
# except Exception as e:
#     print(e)
#     failed_cases.append("go client failed")

try:
    check_python()
except Exception as e:
    print(e)
    failed_cases.append("python client failed")

# try:
#     check_java()
# except Exception as e:
#     print(e)
#     failed_cases.append("java client failed")

if len(failed_cases) != 0:
    print(f"--- client check failed for case\n{failed_cases}")
    sys.exit(1)

subprocess.run(["docker", "compose", "down", "--remove-orphans", "-v"], check=True)
