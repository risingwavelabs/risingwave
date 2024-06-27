import subprocess
import sys
from time import sleep


def check_go():
    subprocess.run(["docker", "compose", "exec", "go-lang", "bash", "-c", "cd /go-client && ./run.sh"],
                   cwd="integration_tests/client-library", check=True)


def check_python():
    subprocess.run(["docker", "compose", "exec", "python", "bash", "-c", "cd /python-client && pip3 install -r requirements.txt && pytest"],
                   cwd="integration_tests/client-library", check=True)


def check_java():
    subprocess.run(["docker", "compose", "exec", "java", "bash", "-c", "apt-get update && apt-get install -y maven"],
                   cwd="integration_tests/client-library", check=True)
    subprocess.run(["docker", "compose", "exec", "java", "bash", "-c", "cd /java-client && mvn clean test"],
                   cwd="integration_tests/client-library", check=True)


def check_nodejs():
    subprocess.run(
        ["docker", "compose", "exec", "nodejs", "bash", "-c", "cd /nodejs-client && npm install && npm test"],
        cwd="integration_tests/client-library", check=True)


def check_php():
    subprocess.run(
        ["docker", "compose", "exec", "php", "bash", "-c", "cd /php-client && phpunit tests/RWClientTest.php"],
        cwd="integration_tests/client-library", check=True)


def check_ruby():
    subprocess.run(["docker", "compose", "exec", "ruby", "bash", "-c", "cd /ruby-client && ruby test/crud_test.rb"],
                   cwd="integration_tests/client-library", check=True)


def check_spring_boot():
    subprocess.run(["docker", "compose", "exec", "spring-boot", "bash", "-c", "cd /spring-boot-client && mvn spring-boot:run"],
                   cwd="integration_tests/client-library", check=True)

subprocess.run(["docker", "compose", "up", "-d"],
               cwd="integration_tests/client-library", check=True)
sleep(10)

failed_cases = []

for client in ['go', 'python', 'java', 'nodejs', 'php', 'ruby', 'spring-boot']:
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
        elif client == 'php':
            check_php()
        elif client == 'ruby':
            check_ruby()
        elif client == 'spring-boot':
            check_spring_boot()
    except Exception as e:
        print(e)
        failed_cases.append(f"{client} client failed")

if len(failed_cases) != 0:
    print(f"--- client check failed for case\n{failed_cases}")
    sys.exit(1)
