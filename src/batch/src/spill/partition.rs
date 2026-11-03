// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Partitioned spilling shared by the batch hash aggregation, hash join and sort executors.
//!
//! When an executor runs out of memory, it writes its buffered state and remaining inputs into
//! [`SPILL_PARTITION_NUM`] partitions, then processes each partition with a sub-executor of the
//! same kind. A sub-executor that runs out of memory spills again, up to [`MAX_SPILL_DEPTH`]
//! levels.

use std::hash::BuildHasher;
use std::sync::Arc;

use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use twox_hash::XxHash64;

use super::spill_op::{SpillBackend, SpillOp};
use crate::error::{BatchError, Result};
use crate::executor::BoxedDataChunkStream;
use crate::monitor::BatchSpillMetrics;

const SPILL_PARTITION_NUM: usize = 20;

/// Partitions no larger than this are processed in memory without checking the memory limit.
const SPILL_AT_LEAST_MEMORY: u64 = 1024 * 1024;

/// Maximum number of nested spill levels. Each level divides evenly distributed input by
/// [`SPILL_PARTITION_NUM`], so a few levels cover any realistic input. Input skewed on a single
/// key cannot be divided at all, so it fails with [`BatchError::OutOfMemory`] at this depth
/// instead of re-spilling until the stack overflows.
const MAX_SPILL_DEPTH: usize = 4;

/// How the rows of a spilled input are assigned to partitions.
pub enum Partitioning {
    /// Rows with equal values in these columns go to the same partition. All hash-partitioned
    /// inputs of one spill share a hasher, so equal keys from different inputs also meet in the
    /// same partition.
    Hash(Vec<usize>),
    /// Rows are spread evenly over the partitions.
    RoundRobin,
}

/// Spill settings of an executor. Sub-executors that process spilled partitions get theirs from
/// [`SpilledPartitions::read`].
#[derive(Clone)]
pub struct SpillContext {
    /// `None` if spilling is disabled.
    backend: Option<SpillBackend>,
    metrics: Arc<BatchSpillMetrics>,
    /// Size of the spilled partition the executor reads, or `None` for a top-level executor.
    partition_size: Option<u64>,
    /// Number of spills that produced the executor's input.
    depth: usize,
}

impl SpillContext {
    pub fn new(backend: Option<SpillBackend>, metrics: Arc<BatchSpillMetrics>) -> Self {
        Self {
            backend,
            metrics,
            partition_size: None,
            depth: 0,
        }
    }

    /// Whether `mem_ctx` exceeds its memory limit. Small partitions never do.
    pub fn over_budget(&self, mem_ctx: &MemoryContext) -> bool {
        !self
            .partition_size
            .is_some_and(|size| size <= SPILL_AT_LEAST_MEMORY)
            && !mem_ctx.check_memory_usage()
    }

    /// Returns `Ok(true)` if the executor should spill now, or an error if it is over budget but
    /// cannot spill.
    pub fn should_spill(&self, mem_ctx: &MemoryContext) -> Result<bool> {
        if !self.over_budget(mem_ctx) {
            return Ok(false);
        }
        if self.backend.is_some() {
            if self.depth < MAX_SPILL_DEPTH {
                return Ok(true);
            }
            warn!(
                depth = self.depth,
                "spilled partition still exceeds the memory limit, input is likely skewed on a single key"
            );
        }
        Err(BatchError::OutOfMemory(mem_ctx.mem_limit()))
    }

    /// Starts spilling `N` inputs, each with its data types and partitioning.
    pub async fn start<const N: usize>(
        &self,
        identity: &str,
        chunk_size: usize,
        inputs: [(Vec<DataType>, Partitioning); N],
    ) -> Result<SpillWriter<N>> {
        let Some(backend) = self.backend.clone() else {
            bail!("spill is disabled for {identity}");
        };
        info!(
            identity,
            depth = self.depth,
            "batch executor starts to spill"
        );
        // A fresh hasher per spill keeps rows that collided at this level from colliding again
        // when a partition is spilled recursively.
        let id: u128 = rand::random();
        let op = SpillOp::create(format!("{identity}-{id:032x}/"), backend)?;
        let mut writers = Vec::with_capacity(N);
        for (input, (data_types, partitioning)) in inputs.into_iter().enumerate() {
            let mut partitions = Vec::with_capacity(SPILL_PARTITION_NUM);
            for partition in 0..SPILL_PARTITION_NUM {
                partitions.push(PartitionWriter {
                    writer: op.writer_with(&file_name(input, partition)).await?,
                    builder: DataChunkBuilder::new(data_types.clone(), chunk_size),
                });
            }
            writers.push(InputWriter {
                partitioning,
                partitions,
                next_round_robin: 0,
            });
        }
        Ok(SpillWriter {
            op,
            hasher: SpillBuildHasher(id as u64),
            inputs: writers.try_into().unwrap_or_else(|_| unreachable!()),
            ctx: self.clone(),
        })
    }
}

/// Writes inputs into spill partitions. Call [`Self::finish`] before reading them back.
pub struct SpillWriter<const N: usize> {
    op: SpillOp,
    hasher: SpillBuildHasher,
    inputs: [InputWriter; N],
    ctx: SpillContext,
}

struct InputWriter {
    partitioning: Partitioning,
    partitions: Vec<PartitionWriter>,
    next_round_robin: usize,
}

struct PartitionWriter {
    writer: opendal::Writer,
    builder: DataChunkBuilder,
}

impl<const N: usize> SpillWriter<N> {
    /// Appends the visible rows of `chunk` to `input`.
    pub async fn write(&mut self, input: usize, chunk: DataChunk) -> Result<()> {
        let w = &mut self.inputs[input];
        let partition_of: Vec<usize> = match &w.partitioning {
            Partitioning::Hash(key_indices) => chunk
                .get_hash_values(key_indices, self.hasher)
                .into_iter()
                .map(|hash| hash.value() as usize % SPILL_PARTITION_NUM)
                .collect(),
            Partitioning::RoundRobin => (chunk.visibility().iter())
                .map(|visible| {
                    let partition = w.next_round_robin;
                    if visible {
                        w.next_round_robin = (partition + 1) % SPILL_PARTITION_NUM;
                    }
                    partition
                })
                .collect(),
        };
        let (columns, vis) = chunk.into_parts_v2();
        for (partition, p) in w.partitions.iter_mut().enumerate() {
            let vis = &vis & Bitmap::from_iter(partition_of.iter().map(|&x| x == partition));
            for chunk in p
                .builder
                .append_chunk(DataChunk::from_parts(columns.clone(), vis))
            {
                SpillOp::write_chunk(&mut p.writer, chunk, &self.ctx.metrics).await?;
            }
        }
        Ok(())
    }

    /// Flushes and closes all partition files.
    pub async fn finish(mut self) -> Result<SpilledPartitions<N>> {
        for p in self.inputs.iter_mut().flat_map(|w| &mut w.partitions) {
            if let Some(chunk) = p.builder.consume_all() {
                SpillOp::write_chunk(&mut p.writer, chunk, &self.ctx.metrics).await?;
            }
            p.writer.close().await?;
        }
        Ok(SpilledPartitions {
            op: self.op,
            ctx: self.ctx,
        })
    }
}

/// Spilled partitions of `N` inputs. The files are removed when this is dropped.
pub struct SpilledPartitions<const N: usize> {
    op: SpillOp,
    ctx: SpillContext,
}

impl<const N: usize> SpilledPartitions<N> {
    pub fn partition_num(&self) -> usize {
        SPILL_PARTITION_NUM
    }

    /// Reads back one partition of each input, along with the spill context for the
    /// sub-executor that processes them.
    pub async fn read(
        &self,
        partition: usize,
    ) -> Result<([BoxedDataChunkStream; N], SpillContext)> {
        let mut size = 0;
        let mut streams = Vec::with_capacity(N);
        for input in 0..N {
            let name = file_name(input, partition);
            size += self.op.stat(&name).await?.content_length();
            let reader = self.op.reader_with(&name).await?;
            streams.push(SpillOp::read_stream(reader, self.ctx.metrics.clone()));
        }
        let ctx = SpillContext {
            partition_size: Some(size),
            depth: self.ctx.depth + 1,
            ..self.ctx.clone()
        };
        Ok((streams.try_into().unwrap_or_else(|_| unreachable!()), ctx))
    }

    /// Removes the files of a partition that has been processed.
    pub async fn clear(&self, partition: usize) -> Result<()> {
        for input in 0..N {
            self.op.delete(&file_name(input, partition)).await?;
        }
        Ok(())
    }
}

fn file_name(input: usize, partition: usize) -> String {
    format!("input{input}-p{partition}")
}

#[derive(Clone, Copy)]
struct SpillBuildHasher(u64);

impl BuildHasher for SpillBuildHasher {
    type Hasher = XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash64::with_seed(self.0)
    }
}
