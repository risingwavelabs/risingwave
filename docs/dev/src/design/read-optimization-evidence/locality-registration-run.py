from pathlib import Path
import subprocess,os,json,time,hashlib,re
root=Path(__file__).resolve().parent;repo=root.parent/'risingwave';binary=root/'paired-bin'
records=[]
cases=[(j,d,c) for j in ['semi','inner'] for d in [0,2000] for c in [2048]]
def metrics(text):
 out={}
 for line in text.splitlines():
  if not line or line.startswith('#'):continue
  key,val=line.rsplit(' ',1)
  try:out[key]=float(val)
  except ValueError:pass
 return out
for round in range(0,4):
 for idx,(join,delay,capacity) in enumerate(cases):
  arms=['baseline','registration'] if (round+idx)%2==0 else ['registration','baseline']
  for arm in arms:
   env=os.environ.copy()
   for k in ['RW_LOCALITY_SCOPED_PLAN','RW_LOCALITY_INCLUDE_RESUMED','RW_LOCALITY_PIPELINE','RW_PLUG_REGISTRATION']:env.pop(k,None)
   env.update(RW_CHAIN_KEYS=str(16384 if join=='semi' else 65536),RW_CHAIN_CACHE_MB='256',RW_CHAIN_DELAY_US=str(delay),RW_CHAIN_JOIN=join,RW_CHAIN_PERMITS=str(capacity))
   if arm=='registration':env.update(RW_PLUG_REGISTRATION='1')
   file=root/f'run-{round}-{join}-{delay}-{capacity}-{arm}.log';start=time.monotonic()
   with file.open('w') as log:
    subprocess.run(['/bin/bash','-c','source ../check-env.sh\nexec ../locality-registration/paired-bin locality_join_read_pattern --ignored --nocapture --test-threads=1'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=150)
   data={}
   for line in file.read_text().splitlines():
    for prefix,name in [('CHAIN_SSTS ','ssts'),('CHAIN_RESULT ','result'),('CHAIN_READ_METRICS ','reads'),('CHAIN_JOIN_METRICS ','join_metrics')]:
     if line.startswith(prefix):data[name]=json.loads(line[len(prefix):])
   assert all(k in data for k in ['ssts','result','reads','join_metrics']),file
   data['reads']=metrics(data['reads']);data['join_metrics']=metrics(data['join_metrics']);data.update(arm=arm,round=round,log=file.name)
   assert data['result']['get']>0
   total=sum(v for k,v in data['join_metrics'].items() if k.startswith('stream_join_lookup_total_count') and 'side="right"' in k)
   misses=sum(v for k,v in data['join_metrics'].items() if k.startswith('stream_join_lookup_miss_count') and 'side="right"' in k)
   assert total==65536 and misses==int(env["RW_CHAIN_KEYS"]),(total,misses)
   records.append(data)
   (root/'results.json').write_text(json.dumps({'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'records':records},indent=2)+'\n')
   print(json.dumps(dict(round=round,case=[join,delay,capacity],arm=arm,get=data['result']['get'],bytes=data['result']['bytes'],ms=data['result']['total_us']/1000,seconds=time.monotonic()-start)),flush=True)
