`ss_bench` is used to benchmark the performance of the state store. In this doc, we first show a usage example and then describe each provided parameter.

# Usage Example

```shell
~/code/risingwave/rust: cargo run --bin ss-bench -- \
 --benchmarks "writebatch,getseq,getrandom,prefixscanrandom" \
 --batch-size 1000 \
 --writes 10000 \
 --reads 500 \
 --concurrency-num 4 \
 --statistics
```

# Parameters

## State Store

### Backend Types  (`--store`)

- `In-memory`
  
  - Format: `in-memory`(or `in_memory`)
  - Default

- `Hummock+MinIO`
  
  - Format: `hummock+minio://key:secret@address:port/bucket`
  - Example: `--store hummock+minio://INTEGRATION_TEST_ACCESS_KEY:INTEGRATION_TEST_SECRET_KEY@127.0.0.1:9000/myminio`

- `Hummock+S3`
  
  - Format: `hummock+s3://bucket`
  - Example: `hummock+s3://s3-ut`
  - Notice: some environment variables are required to be set
    - `S3_TEST_REGION`
    - `S3_TEST_ACCESS_KEY`
    - `S3_TEST_SECRET_KEY`

- `TiKV`
  
  - Foramt: `tikv://pd_address:port`
  - Example: `--store tikv://127.0.0.1:2379`

- `RocksDB`
  
  - Foramt: TBD

### Hummock Configurations

- `--table-size-mb`
  
  - Size (MB) of an SSTable
  - Default: 256

- `--block-size-kb`
  
  - Size (KB) of a block in an SSTable
  - Default: 64

- `--bloom-false-positive`
  
  - Bloom Filter false positive rate
  - Default: 0.1

- `--checksum-algo`
  
  - Checksum algorithm
  
  - Options:
    
    - `crc32c`: default
    - `xxhash`

## Operations

### Concurrency Number (`--concurrency-num`)

- Concurrency number of each operation. Workloads of each concurrency are almost the same.
- Default: 1

### Operation Types (`--benchmarks`)

Comma-separated list of operations to run in the specified order. Following operations are supported:

- `writebatch`: write N key/values in sequential key order in async mode.
- `deleterandom`: delete N key/values in random order. May delete a key/values many times even it has been deleted at the first times.
- `getrandom`: read N times in random order. May read a key/values many times.
- `getseq`: read N times sequentially.
- `prefixscanrandom`: prefix scan N times in random order.

Example: `--benchmarks "writebatch,prefixscanrandom,getrandom"`

### Operation Numbers

- `--num`

  - Number of key/values to place in database.
  - Default: 1000000

- `--deletes`

  - Number of deleted keys. If negative, do `--num` deletions.
  - Default: -1

- `--reads`

  - Number of read keys. If negative, do `--num` reads.
  - Default: -1

- `--writes`

  - Number of written key/values. If negative, do `--num` reads.
  - Default: -1

- `--batch-size`

  - **Max** number of key/values in a batch. When the key/values are not evenly divided by the `--batch-size`, the last batch will be the remainder.
  - Default: 100

## Key/values Sizes

- `--key-size`
  
  - Size (bytes) of each user_key (non-prefix part of a key).
  - Default: 16

- `--key-prefix-size`
  
  - Size (bytes) of each prefix.
  - Default: 5

- `--keys_per_prefix`
  
  - Control **average** number of keys generated per prefix.
  - Default: 10

- `--value-size`
  
  - Size (bytes) of each value.
  - Default: 100

# Flag

- `--statistics`
  - Detailed statistics of storage backend

# Metrics

- Letancy (`min/mean/P50/P95/P99/max/std_dev`)
- Throughput (`QPS/OPS`)
