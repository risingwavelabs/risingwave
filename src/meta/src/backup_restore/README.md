# Meta Service Backup and Recovery

## How to take a meta snapshot
From the running meta service cluster
```bash
risectl meta backup-meta
```

## How to restore meta service cluster
1. Stop the running meta service cluster, if any.
2. Create a new empty meta store.
3. Restore specified meta snapshot to this meta store.
```bash
backup-restore <options, see -h for detail>
```
4. Config meta service cluster to use the new meta store.

### Caveat
The meta service backup/recovery procedure **doesn't** replicate SSTs in object store. 
So always make sure the underlying SST object store is writable to at most one running cluster at any time.
Otherwise, the SST object store will face the risk of data corruption.