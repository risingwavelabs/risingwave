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

//! Hash partitioning of spilled inputs, shared by hash join and hash aggregation.
//!
//! Rows with equal keys go to the same partition, so each partition can be processed on its own.
//! Every spill level hashes with its own fixed seed: rows that shared a partition at one level are
//! spread again at the next, and the same input always splits the same way.

use std::hash::{BuildHasher, Hasher};

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use twox_hash::XxHash64;

use super::spill_file::{Charge, SpillDir, SpillFile, SpillFileWriter};
use crate::error::{BatchError, Result};

/// Number of partitions of each input per spill.
pub const PARTITION_NUM: usize = 20;

/// Bytes of buffered chunks after which a [`PartitionWriter`] partitions them, even before they
/// hold `chunk_size` rows.
pub const PENDING_BYTES: usize = 1 << 20;

const SEED: u64 = 0x2d35_8dcc_aa6c_78a5;

#[derive(Clone, Copy)]
struct LevelHasher(u64);

impl LevelHasher {
    fn new(level: u32) -> Self {
        Self(SEED ^ u64::from(level).wrapping_mul(0x9e37_79b9_7f4a_7c15))
    }
}

impl BuildHasher for LevelHasher {
    type Hasher = XxHash64;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash64::with_seed(self.0)
    }
}

/// Writes `N` inputs into [`PARTITION_NUM`] partitions by the hash of their key columns. Equal
/// keys of different inputs go to the same partition.
///
/// The chunks of an input are buffered until they hold `chunk_size` rows or [`PENDING_BYTES`], so
/// that partitions are written in chunks of many rows even when the input chunks are small, for
/// example after a hash shuffle. The buffered chunks are charged to the memory context.
pub struct PartitionWriter<const N: usize> {
    hasher: LevelHasher,
    chunk_size: usize,
    inputs: [PartitionInput; N],
}

struct PartitionInput {
    key_indices: Vec<usize>,
    data_types: Vec<DataType>,
    files: Vec<SpillFileWriter>,
    pending: Vec<DataChunk>,
    pending_rows: usize,
    pending_bytes: usize,
    charge: Charge,
}

impl<const N: usize> PartitionWriter<N> {
    /// `level` counts the spills that produced the rows being partitioned.
    pub fn new(
        dir: &SpillDir,
        level: u32,
        key_indices: [Vec<usize>; N],
        data_types: [Vec<DataType>; N],
        chunk_size: usize,
        mem_ctx: &MemoryContext,
    ) -> Result<Self> {
        let mut inputs = Vec::with_capacity(N);
        for (key_indices, data_types) in key_indices.into_iter().zip_eq_fast(data_types) {
            let files = (0..PARTITION_NUM)
                .map(|_| dir.create_file(mem_ctx))
                .collect::<Result<_>>()?;
            inputs.push(PartitionInput {
                key_indices,
                data_types,
                files,
                pending: vec![],
                pending_rows: 0,
                pending_bytes: 0,
                charge: Charge::new(mem_ctx.clone()),
            });
        }
        Ok(Self {
            hasher: LevelHasher::new(level),
            chunk_size,
            inputs: inputs.try_into().unwrap_or_else(|_| unreachable!()),
        })
    }

    /// The partition of each row of `chunk` from `input`, by position.
    pub fn partitions_of(&self, input: usize, chunk: &DataChunk) -> Vec<usize> {
        partitions_of(self.hasher, &self.inputs[input].key_indices, chunk)
    }

    /// Writes the visible rows of `chunk` from `input`.
    pub async fn write(&mut self, input: usize, chunk: &DataChunk) -> Result<()> {
        if chunk.cardinality() == 0 {
            return Ok(());
        }
        let pending = &mut self.inputs[input];
        pending.pending_rows += chunk.cardinality();
        pending.pending_bytes += chunk.estimated_heap_size();
        pending.charge.set(pending.pending_bytes);
        pending.pending.push(chunk.clone());
        if pending.pending_rows >= self.chunk_size || pending.pending_bytes >= PENDING_BYTES {
            self.flush(input).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, input: usize) -> Result<()> {
        let hasher = self.hasher;
        let chunk_size = self.chunk_size;
        let input = &mut self.inputs[input];
        let chunks = std::mem::take(&mut input.pending);
        let partitions: Vec<_> = chunks
            .iter()
            .map(|chunk| partitions_of(hasher, &input.key_indices, chunk))
            .collect();
        for (partition, file) in input.files.iter_mut().enumerate() {
            let mut builder = DataChunkBuilder::new(input.data_types.clone(), chunk_size);
            for (chunk, partitions) in chunks.iter().zip_eq_fast(&partitions) {
                let vis = chunk.visibility()
                    & Bitmap::from_iter(partitions.iter().map(|p| *p == partition));
                for chunk in builder.append_chunk(chunk.with_visibility(vis)) {
                    file.write_chunk(&chunk).await?;
                }
            }
            if let Some(chunk) = builder.consume_all() {
                file.write_chunk(&chunk).await?;
            }
        }
        input.pending_rows = 0;
        input.pending_bytes = 0;
        input.charge.set(0);
        Ok(())
    }

    /// Finishes the files, returning the file of each input for every partition.
    pub async fn finish(mut self) -> Result<Vec<[SpillFile; N]>> {
        for input in 0..N {
            self.flush(input).await?;
        }
        let mut partitions: Vec<Vec<SpillFile>> =
            (0..PARTITION_NUM).map(|_| Vec::with_capacity(N)).collect();
        for input in self.inputs {
            for (partition, file) in input.files.into_iter().enumerate() {
                partitions[partition].push(file.finish().await?);
            }
        }
        Ok(partitions
            .into_iter()
            .map(|files| files.try_into().unwrap_or_else(|_| unreachable!()))
            .collect())
    }
}

/// The hash covers whether each key is NULL, besides its value: a NULL is hashed like one of the
/// values of its type (an int4 NULL like -16), from which partitioning could then never split it.
fn partitions_of(hasher: LevelHasher, key_indices: &[usize], chunk: &DataChunk) -> Vec<usize> {
    let mut states = vec![hasher.build_hasher(); chunk.capacity()];
    for &key in key_indices {
        let column = chunk.column_at(key);
        column.hash_vec(&mut states, chunk.visibility());
        for (state, not_null) in states.iter_mut().zip_eq_fast(column.null_bitmap().iter()) {
            state.write_u8(not_null as u8);
        }
    }
    states
        .into_iter()
        .map(|state| state.finish() as usize % PARTITION_NUM)
        .collect()
}

/// Reads `file` back in chunks of `chunk_size` rows.
#[try_stream(boxed, ok = DataChunk, error = BatchError)]
pub async fn read_coalesced(
    file: SpillFile,
    data_types: Vec<DataType>,
    chunk_size: usize,
    mem_ctx: MemoryContext,
) {
    let mut builder = DataChunkBuilder::new(data_types, chunk_size);
    #[for_await]
    for chunk in file.read(&mem_ctx) {
        for chunk in builder.append_chunk(chunk?) {
            yield chunk;
        }
    }
    if let Some(chunk) = builder.consume_all() {
        yield chunk;
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunkTestExt;

    use super::*;

    /// A NULL key is hashed like int4 -16, yet the two go to different partitions at some level.
    #[test]
    fn test_null_and_minus_16_split() {
        let chunk = DataChunk::from_pretty(
            "i
             .
             -16",
        );
        assert!((0..4).any(|level| {
            let partitions = partitions_of(LevelHasher::new(level), &[0], &chunk);
            partitions[0] != partitions[1]
        }));
    }
}
