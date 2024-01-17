import subprocess
import sys
from time import sleep


def check_go():
    subprocess.run(["docker", "compose", "exec", "go-lang", "bash", "-c", "cd /go-client && ./run.sh"], check=True)


def check_python():
    subprocess.run(["docker", "compose", "exec", "python", "bash", "-c",
                    "cd /python-client && pip3 install -r requirements.txt && pytest"], check=True)


def check_java():
    subprocess.run(["docker", "compose", "exec", "java", "bash", "-c", "apt-get update && apt-get install -y maven"],
                   check=True)
    subprocess.run(["docker", "compose", "exec", "java", "bash", "-c", "cd /java-client && mvn clean test"], check=True)


def check_nodejs():
    subprocess.run(
        ["docker", "compose", "exec", "nodejs", "bash", "-c", "cd /nodejs-client && npm install && npm test"],
        check=True)


subprocess.run(["docker", "compose", "up", "-d"], check=True)
sleep(10)

failed_cases = []

for client in ['go', 'python', 'java', 'nodejs']:
    print(f"--- {client} client test")
    try:
        if client == 'go':
            check_go()
        elif client == 'python':
            check_python()
        elif client == 'java':
            check_java()
        elif client == 'nodejs':
            check_nodejs()
    except Exception as e:
        print(e)
        failed_cases.append(f"{client} client failed")

if len(failed_cases) != 0:
    print(f"--- client check failed for case\n{failed_cases}")
    sys.exit(1)

subprocess.run(["docker", "compose", "down", "--remove-orphans", "-v"], check=True)
