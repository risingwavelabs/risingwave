import subprocess
import sys

sub_containers = ['subject1', 'subject2']

failed_cases = []
for sub in sub_containers:
    cmd = f'docker compose logs {sub}'
    output = subprocess.check_output(['docker', 'compose', 'logs', sub])
    output = output.decode('utf-8')
    print(output)

    lines = output.split('\n')
    processed_lines = [line.replace(f'risingwave-compose-{sub}-1  | ', '') for line in lines]
    processed_lines = [line for line in processed_lines if line.strip() != '']
    rows = (len(processed_lines) - 1) / 2
    print(f'{rows} rows in {sub} container')
    if len(processed_lines) < 1:
        failed_cases.append(sub)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
