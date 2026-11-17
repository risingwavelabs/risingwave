from pathlib import Path
import subprocess, os, re, json

root = Path(__file__).resolve().parent
repo = root.parent/'risingwave'
build = (root/'restored-build.log').read_text()
assert 'Finished `test`' in build
paths = re.findall(r'Executable unittests src/lib.rs \(([^)]+)\)',build)
storage = next(p for p in paths if '/risingwave_storage-' in p)
stream = next(p for p in paths if '/risingwave_stream-' in p)
cases = [(storage,'hummock::sstable::forward_sstable_iterator::tests','iterator'),
         (storage,'hummock::sstable_store::tests','store'),
         (storage,'test_backfill_prefetch_modes_are_independent','routing'),
         (stream,'executor::locality_provider::tests','locality'),
         (stream,'executor::hash_join::tests','join'),
         (stream,'locality_join_read_pattern','semi-smoke'),
         (stream,'locality_join_read_pattern','inner-smoke')]
results=[]
for binary, name, label in cases:
    env=os.environ.copy()
    args=['--test-threads=1']
    if 'smoke' in label:
        args += ['--ignored','--nocapture']
        env.update(RW_CHAIN_JOIN=label.split('-')[0],RW_CHAIN_KEYS='256',RW_CHAIN_CACHE_MB='256',RW_CHAIN_DELAY_US='0',RW_CHAIN_PERMITS='2048')
    path=root/f'restored-{label}.log'
    with path.open('w') as log:
        subprocess.run(['/bin/bash','-c','source ../check-env.sh\nexec "$@"','test',binary,name,*args],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=150)
    line=next(l for l in path.read_text().splitlines() if l.startswith('test result:'))
    count=int(re.search(r'(\d+) passed',line)[1]);assert count>0,line
    results.append(dict(label=label,passed=count,result=line))
    print(label,line,flush=True)
(root/'validation.json').write_text(json.dumps(results,indent=2)+'\n')
