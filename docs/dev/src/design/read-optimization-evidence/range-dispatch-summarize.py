from pathlib import Path
from collections import defaultdict
from statistics import median
import json, math

root = Path(__file__).resolve().parent
def stats(xs):
    xs = sorted(xs)
    return {'n': len(xs), 'median': median(xs), 'min': xs[0], 'max': xs[-1], 'p95': xs[math.ceil(.95*len(xs))-1]}
def metric(r, prefix):
    return sum(v for k,v in r['reads'].items() if k.startswith(prefix))

data = json.loads((root/'results.json').read_text())
groups = defaultdict(list)
seeds = {}
for r in data['records']:
    key = r['result']['join']
    if key in seeds: assert r['ssts'] == seeds[key], (key, r['log'])
    else: seeds[key] = r['ssts']
    if r['arm'] == 'range_dispatch':
        assert metric(r, 'object_store_read_scheduler_group_size_sum{') == metric(r, 'object_store_read_scheduler_group_size_count{')
        assert metric(r, 'object_store_read_scheduler_events{event="merged_requests"') == 0
    if r['round'] > 0:
        groups[(key, r['result']['delay_us'], r['arm'])].append(r)
summary = []
for (join, delay, arm), rs in sorted(groups.items()):
    out = dict(join=join, delay_us=delay, arm=arm)
    for key in ['get','bytes','total_us','cache_usage_end','range_buffer_usage_end']:
        out[key] = stats([r['result'][key] for r in rs])
    for name,prefix in [('scheduled','object_store_read_requests{media_type="mem",route="scheduled"'),('direct','object_store_read_requests{media_type="mem",route="direct"'),('oversized','object_store_read_requests{media_type="mem",route="oversized"'),('unbounded','object_store_read_requests{media_type="mem",route="unbounded"'),('merged','object_store_read_scheduler_events{event="merged_requests"')]:
        out[name] = stats([metric(r,prefix) for r in rs])
    for key in ['real','user','sys','max_rss_raw']:
        out['process_'+key] = stats([r['process_time'][key] for r in rs])
    summary.append(out)
    print(join, delay, arm, 'GET', out['get']['median'], 'ms', round(out['total_us']['median']/1000,2), 'range_ms', [round(out['total_us'][s]/1000,2) for s in ['min','max']], 'merged', out['merged']['median'])
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
if (root/'micro-results.json').exists():
    groups = defaultdict(list)
    for r in json.loads((root/'micro-results.json').read_text())['records']:
        groups[(r['scenario'],r['delay_us'],r['arm'])].append(r)
    summary = []
    for (scenario,delay,arm), rs in sorted(groups.items()):
        out = dict(scenario=scenario,delay_us=delay,arm=arm)
        for key in ['gets','bytes','unique_bytes','first_us','total_us','point_us','max_next_us']:
            out[key] = stats([r[key] for r in rs])
        summary.append(out)
    (root/'micro-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
