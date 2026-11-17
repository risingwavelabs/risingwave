from pathlib import Path
import os, subprocess, json, hashlib, time

root = Path(__file__).resolve().parent
repo = root.parent / 'risingwave'
binary = root / 'micro-bin'
arms = ['range_scheduler', 'cold_ranges_scheduler']
records = []
identity = hashlib.sha256(binary.read_bytes()).hexdigest()
for round, scenario in enumerate(["cold16", "warm_gaps", "warm_head", "shared"]):
    order = arms[round % len(arms):] + arms[:round % len(arms)]
    for arm in order:
        env = os.environ.copy()
        env.update(RW_RANGE_SCENARIO=scenario, RW_RANGE_DISK='1')
        env.update(RW_RANGE_ARM=arm, RW_RANGE_TRIALS='20')
        path = root / f'scheduled-disk-{scenario}-{arm}.log'
        start = time.monotonic()
        with path.open('w') as log:
            subprocess.run(['/bin/bash', '-c', 'source ../check-env.sh\nexec ../cold-ranges/micro-bin cold_range_experiment --ignored --nocapture --test-threads=1'], cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=150)
        samples = []
        for line in path.read_text().splitlines():
            if not line.startswith('RANGE_CONTROL '):
                continue
            sample = dict(field.split('=', 1) for field in line.split()[1:])
            for key in sample.keys() - {'arm', 'scenario'}:
                sample[key] = float(sample[key])
            sample.update(round=round)
            samples.append(sample)
        assert len(samples) == 40, (path, len(samples))
        records.extend(samples)
        (root / 'scheduled-disk-results.json').write_text(json.dumps({'binary_sha256': identity, 'records': records}, indent=2) + '\n')
        print(json.dumps(dict(round=round, arm=arm, samples=len(samples), seconds=time.monotonic()-start)), flush=True)
