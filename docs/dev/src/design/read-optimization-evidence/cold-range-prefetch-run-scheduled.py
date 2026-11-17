from pathlib import Path
import subprocess,os,json,time,hashlib
root=Path(__file__).resolve().parent;repo=root.parent/'risingwave';binary=root/'paired-bin'
records=[];sha=hashlib.sha256(binary.read_bytes()).hexdigest()
flags={arm:None for arm in ['range_scheduler','cold_ranges_scheduler']}
cases=[(j,d) for j in ['semi','inner'] for d in [0,2000]]
def metrics(text):
 out={}
 for line in text.splitlines():
  if not line or line.startswith('#'):continue
  key,val=line.rsplit(' ',1)
  try:out[key]=float(val)
  except ValueError:pass
 return out
for round in range(4):
 for idx,(join,delay) in enumerate(cases):
  arms=list(flags);shift=(round+idx)%len(arms);arms=arms[shift:]+arms[:shift]
  for arm in arms:
   env=os.environ.copy()
   for k in [*filter(None,flags.values()),'RW_LOCALITY_PIPELINE','RW_PLUG_REGISTRATION','RW_LOCALITY_SCOPED_PLAN','RW_LOCALITY_INCLUDE_RESUMED','RW_CHAIN_NO_PREFIX_PREFETCH','RW_CHAIN_NO_LOCALITY_PREFETCH','RW_CHAIN_NO_PLUG']:env.pop(k,None)
   env.update(RW_CHAIN_KEYS=str(16384 if join=='semi' else 65536),RW_CHAIN_CACHE_MB='256',RW_CHAIN_DELAY_US=str(delay),RW_CHAIN_JOIN=join,RW_CHAIN_PERMITS='2048')
   env['RW_RANGE_ARM']=arm
   file=root/f'scheduled-{round}-{join}-{delay}-{arm}.log';start=time.monotonic()
   with file.open('w') as log:
    subprocess.run(['/usr/bin/time','-lp','/bin/bash','-c','source ../check-env.sh\nexec ../cold-ranges/paired-bin locality_join_read_pattern --ignored --nocapture --test-threads=1'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=150)
   data={}
   for line in file.read_text().splitlines():
    for prefix,name in [('CHAIN_SSTS ','ssts'),('CHAIN_RESULT ','result'),('CHAIN_READ_METRICS ','reads'),('CHAIN_JOIN_METRICS ','join_metrics')]:
     if line.startswith(prefix):data[name]=json.loads(line[len(prefix):])
   assert all(k in data for k in ['ssts','result','reads','join_metrics']),file
   data['process_time']={}
   for line in file.read_text().splitlines():
    words=line.split()
    if len(words)==2 and words[0] in ['real','user','sys']:data['process_time'][words[0]]=float(words[1])
    if 'maximum resident set size' in line:data['process_time']['max_rss_raw']=int(words[0])
   data['reads']=metrics(data['reads']);data['join_metrics']=metrics(data['join_metrics']);data.update(arm=arm,round=round,log=file.name)
   assert data['result']['get']>0
   total=sum(v for k,v in data['join_metrics'].items() if k.startswith('stream_join_lookup_total_count') and 'side="right"' in k)
   misses=sum(v for k,v in data['join_metrics'].items() if k.startswith('stream_join_lookup_miss_count') and 'side="right"' in k)
   assert total==65536 and misses==int(env['RW_CHAIN_KEYS']),(total,misses)
   records.append(data)
   (root/'scheduled-results.json').write_text(json.dumps({'binary_sha256':sha,'records':records},indent=2)+'\n')
   print(json.dumps(dict(round=round,case=[join,delay],arm=arm,get=data['result']['get'],bytes=data['result']['bytes'],ms=data['result']['total_us']/1000,seconds=time.monotonic()-start)),flush=True)
