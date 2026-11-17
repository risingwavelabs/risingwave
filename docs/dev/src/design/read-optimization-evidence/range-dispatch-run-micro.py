from pathlib import Path
import os, subprocess, json, hashlib, time

root = Path(__file__).resolve().parent
repo = root.parent / 'risingwave'
binary = root.parent / 'cargo-target/debug/deps/risingwave_storage-b78400c78dd896c6'
arms = ['range_direct', 'range_dispatch', 'range_scheduler', 'blockwise_scheduler']
records = []
identity = hashlib.sha256(binary.read_bytes()).hexdigest()
for round in range(4):
    order = arms[round:] + arms[:round]
    for arm in order:
        env = os.environ.copy()
        env.pop('RW_RANGE_SCENARIO', None)
        env.update(RW_RANGE_ARM=arm, RW_RANGE_TRIALS='10')
        path = root / f'micro-{round}-{arm}.log'
        start = time.monotonic()
        with path.open('w') as log:
            subprocess.run(['/bin/bash', '-c', 'source ../check-env.sh\nexec ../cargo-target/debug/deps/risingwave_storage-b78400c78dd896c6 range_scheduler_experiment --ignored --nocapture --test-threads=1'], cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=150)
        samples = []
        for line in path.read_text().splitlines():
            if not line.startswith('RANGE_CONTROL '):
                continue
            sample = dict(field.split('=', 1) for field in line.split()[1:])
            for key in sample.keys() - {'arm', 'scenario'}:
                sample[key] = float(sample[key])
            sample.update(round=round)
            samples.append(sample)
        assert len(samples) == 240, (path, len(samples))
        records.extend(samples)
        (root / 'micro-results.json').write_text(json.dumps({'binary_sha256': identity, 'records': records}, indent=2) + '\n')
        print(json.dumps(dict(round=round, arm=arm, samples=len(samples), seconds=time.monotonic()-start)), flush=True)
