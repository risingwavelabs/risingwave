from pathlib import Path
import os, subprocess, json, hashlib

root = Path(__file__).resolve().parent
repo = root.parent / 'risingwave'
binary = root.parent / 'cargo-target/debug/deps/risingwave_storage-b78400c78dd896c6'
identity = hashlib.sha256(binary.read_bytes()).hexdigest()
cases = [(arm, scenario) for arm in ['blockwise_direct','blockwise_dispatch','blockwise_scheduler'] for scenario in ['cold16','warm_gaps']]
cases += [('range_scheduler','shared'), ('range_scheduler','existing_point')]
records = []
for round in range(2):
    for arm, scenario in (cases if round == 0 else list(reversed(cases))):
        env = os.environ.copy()
        env.update(RW_RANGE_ARM=arm,RW_RANGE_SCENARIO=scenario,RW_RANGE_TRIALS='20')
        path = root / f'control-{round}-{arm}-{scenario}.log'
        with path.open('w') as log:
            subprocess.run(['/bin/bash','-c','source ../check-env.sh\nexec ../cargo-target/debug/deps/risingwave_storage-b78400c78dd896c6 range_scheduler_experiment --ignored --nocapture --test-threads=1'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=150)
        samples = []
        for line in path.read_text().splitlines():
            if line.startswith('RANGE_CONTROL '):
                sample = dict(field.split('=',1) for field in line.split()[1:])
                for key in sample.keys()-{'arm','scenario'}:
                    sample[key] = float(sample[key])
                sample['round'] = round
                samples.append(sample)
        assert len(samples)==40,(path,len(samples))
        records.extend(samples)
        (root/'control-results.json').write_text(json.dumps({'binary_sha256':identity,'records':records},indent=2)+'\n')
        print(round,arm,scenario,len(samples),flush=True)
