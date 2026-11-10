"""Summarize one replay results-* directory; fail on changing output/read byte counts."""
import json
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1])
rows = [json.loads(p.read_text()) for p in sorted(root.glob('run-*/result.json'))]
config = json.loads((root / 'config.json').read_text())
arms = config.get('schedulers', [False, True])
assert len(rows) == len(arms) * config['repeats'], 'incomplete matrix'
assert {r['rows'] for r in rows} == {config['keys'] * config['rows_per_key'] * config.get('probe_rounds', 1)}
bytes_read = {r['metrics']['object_store_read_bytes'] for r in rows}
assert len(bytes_read) == 1, f'read bytes changed: {bytes_read}'
print('| Scheduler | Runs | Physical reads min–max | Read bytes | Merged requests min–max | Median probe ms |')
print('|---|---:|---:|---:|---:|---:|')
for enabled in arms:
    arm = [r for r in rows if r['scheduler'] == enabled]
    assert len(arm) == config['repeats']
    physical, saved = [], []
    for r in arm:
        m = r['metrics']
        members = m['object_store_read_scheduler_group_size_sum']
        groups = m['object_store_read_scheduler_group_size_count']
        logical = m['object_store_read_requests']
        if config['warm_every_nth_block'] == 1:
            assert logical == r['physical_reads'] == members == groups == m['object_store_read_bytes'] == 0, 'all-hot case issued I/O'
        else:
            assert logical > 0 and r['physical_reads'] > 0, 'no physical read exposure'
            assert groups > 0 if enabled else groups == 0, 'scheduler arm was not exercised'
        assert logical - r['physical_reads'] == members - groups, 'read accounting does not reconcile'
        physical.append(r['physical_reads'])
        saved.append(int(members - groups))
    print(f"| {enabled} | {len(arm)} | {min(physical)}–{max(physical)} | {int(next(iter(bytes_read))):,} | {min(saved)}–{max(saved)} | {1000 * statistics.median(r['elapsed_s'] for r in arm):.3f} |")
print('\nAll output checks and equal read-byte gates passed. Scheduler grouping and wall time may vary.')
