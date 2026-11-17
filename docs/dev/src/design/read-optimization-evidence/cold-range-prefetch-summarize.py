from pathlib import Path
from collections import defaultdict
from statistics import median
import json, math

root=Path(__file__).resolve().parent
def stats(values):
    values=sorted(values)
    return dict(n=len(values),median=median(values),min=values[0],max=values[-1],p95=values[math.ceil(.95*len(values))-1])

seeds={}
for source in ['results','scheduled-results']:
    path=root/f'{source}.json'
    if not path.exists():continue
    groups=defaultdict(list)
    for r in json.loads(path.read_text())['records']:
        key=r['result']['join']
        if key in seeds:assert r['ssts']==seeds[key],r['log']
        else:seeds[key]=r['ssts']
        for name,value in r['reads'].items():
            if name.startswith(('object_store_read_scheduler_pending{','object_store_read_scheduler_waiting{','object_store_read_scheduler_queued{','object_store_read_scheduler_reserved{','object_store_read_scheduler_in_flight{','object_store_read_scheduler_groups{','object_store_read_in_flight{')):
                assert value==0,(r['log'],name,value)
        routes={k:v for k,v in r['reads'].items() if k.startswith('object_store_read_requests{')}
        assert routes,(r['log'],'missing routes')
        expected='direct' if r['arm'].endswith('_direct') else 'scheduled'
        assert all(f'route="{expected}"' in k or v==0 for k,v in routes.items()),(r['log'],routes)
        if r['round']>0:groups[(key,r['result']['delay_us'],r['arm'])].append(r)
    out=[]
    for (join,delay,arm),rs in sorted(groups.items()):
        row=dict(join=join,delay_us=delay,arm=arm)
        for key in ['get','bytes','total_us','cache_usage_end','range_buffer_usage_end']:
            row[key]=stats([r['result'][key] for r in rs])
        out.append(row)
        print(source,join,delay,arm,'GET',row['get']['median'],'ms',round(row['total_us']['median']/1000,2))
    (root/f'{source}-summary.json').write_text(json.dumps(out,indent=2)+'\n')

for source in ['micro-results','disk-results','scheduled-micro-results','scheduled-disk-results']:
    path=root/f'{source}.json'
    if not path.exists():continue
    groups=defaultdict(list)
    for r in json.loads(path.read_text())['records']:groups[(r['scenario'],r['delay_us'],r['arm'])].append(r)
    out=[]
    for (scenario,delay,arm),rs in sorted(groups.items()):
        row=dict(scenario=scenario,delay_us=delay,arm=arm)
        for key in ['gets','bytes','unique_bytes','first_us','total_us','point_us','max_next_us']:
            row[key]=stats([r[key] for r in rs])
        out.append(row)
    (root/f'{source}-summary.json').write_text(json.dumps(out,indent=2)+'\n')
