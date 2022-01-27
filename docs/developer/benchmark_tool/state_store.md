ss_bench is used to benchmark the performance of the state store. In this doc, we first show a usage example and then describe each provided parameter.

# Usage Example

```shell
~/code/risingwave/rust: cargo run --bin ss-bench --\
 --benchmarks "writebatch,prefixscanrandom,getrandom"\
 --kvs-per-batch 1000\
 --iterations 100\
 --concurrency-num 4
```

# Parameters

## State Store

### Type  (`--store`)

- `In-memory`
  
  - Format: `in-memory`(or `in_memory`)
  - Default value

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
  - Default value: 256

- `--block-size-kb`
  
  - Size (KB) of a block in an SSTable
  - Default value: 64

- `--bloom-false-positive`
  
  - Bloom Filter false positive rate
  - Default value: 0.1

- `--checksum-algo`
  
  - Checksum algorithm
  
  - Options:
    
    - `crc32c`: default value
    - `xxhash`

## Benchmarks

### Number (`--iterations`)

- The times that each benchmark has been executed
- Default value: 10

### Concurrency Number (`--concurrency-num`)

- The concurrency number of each benchmark
- Default value: 1

### Benchmark Type (`--benchmarks`)

- `writebatch`: write `iterations` KV pairs in sequential key order in async mode
- `getrandom`: read `iterations` times in random order
- `prefixscanrandom`: prefix scan `iterations` times in random order

The benchmarks could be a combination of multiple consequent benchmarks. Example: `--benchmarks "writebatch,prefixscanrandom,getrandom"`

## Batch Configurations

- `--key-size`
  
  - The size (bytes) of the non-prefix part of a key
  - Default value: 10

- `--key-prefix-size`
  
  - The size (bytes) of a prefix
  - Default value: 5

- `--key-prefix-frequency`
  
  - The number of keys with some a prefix in a batch
  - Default value: 10

- `--value-size`
  
  - The length (bytes) of a value in a KV pair
  - Default value: 10

- `--kvs-per-batch`
  
  - The number of KV pairs in a batch
  - Default value: 1000

# Metrics

- Letancy (`min/mean/P50/P95/P99/max/std_dev`)
- Throughput (`QPS/OPS`)
