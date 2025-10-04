// Copyright 2025 RisingWave Labs
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

use risingwave_hummock_sdk::vector_index::VectorFileInfo;

use crate::hummock::vector::file::{VectorBlock, VectorBlockBuilder, VectorBlockMeta};
use crate::hummock::vector::monitor::VectorStoreCacheStats;
use crate::hummock::{HummockResult, SstableStoreRef, VectorBlockHolder};
use crate::vector::VectorRef;
use crate::vector::hnsw::VectorAccessor;

pub(crate) mod file;
pub(crate) mod monitor;
pub(crate) mod writer;

pub struct VectorBlockAccessor {
    block: VectorBlockHolder,
    idx: usize,
}

impl VectorAccessor for VectorBlockAccessor {
    fn vec_ref(&self) -> VectorRef<'_> {
        self.block.vec_ref(self.idx)
    }

    fn info(&self) -> &[u8] {
        self.block.info(self.idx)
    }
}

pub enum EnumVectorAccessor<'a> {
    Builder(&'a VectorBlockBuilder, usize),
    BlockRef(&'a VectorBlock, usize),
    BlockHolder(VectorBlockAccessor),
}

impl VectorAccessor for EnumVectorAccessor<'_> {
    fn vec_ref(&self) -> VectorRef<'_> {
        match self {
            EnumVectorAccessor::Builder(builder, offset) => builder.vec_ref(*offset),
            EnumVectorAccessor::BlockRef(block, offset) => block.vec_ref(*offset),
            EnumVectorAccessor::BlockHolder(accessor) => accessor.vec_ref(),
        }
    }

    fn info(&self) -> &[u8] {
        match self {
            EnumVectorAccessor::Builder(builder, offset) => builder.info(*offset),
            EnumVectorAccessor::BlockRef(block, offset) => block.info(*offset),
            EnumVectorAccessor::BlockHolder(accessor) => accessor.info(),
        }
    }
}

pub async fn get_vector_block(
    sstable_store: &SstableStoreRef,
    files: &[VectorFileInfo],
    idx: usize,
    stats: &mut VectorStoreCacheStats,
) -> HummockResult<VectorBlockAccessor> {
    let vector_file = search_vector_files(files, idx);
    let meta = sstable_store
        .get_vector_file_meta(vector_file, stats)
        .await?;
    let (block_meta, block_idx, offset) = search_blocks(&meta.block_metas, idx);
    let block = sstable_store
        .get_vector_block(vector_file, block_idx, block_meta, stats)
        .await?;
    Ok(VectorBlockAccessor { block, idx: offset })
}

fn search_vector_files(files: &[VectorFileInfo], idx: usize) -> &VectorFileInfo {
    let (file_idx, _) = search_vector(files, idx, |file| file.start_vector_id);
    &files[file_idx]
}

fn search_blocks(blocks: &[VectorBlockMeta], idx: usize) -> (&VectorBlockMeta, usize, usize) {
    let (block_idx, offset) = search_vector(blocks, idx, |block| block.start_vector_id);
    (&blocks[block_idx], block_idx, offset)
}

/// return (`holder_idx`, `offset` inside holder)
pub fn search_vector<T>(
    vector_holders: &[T],
    idx: usize,
    get_start_vector_id: impl Fn(&T) -> usize,
) -> (usize, usize) {
    let holder_idx = vector_holders
        .partition_point(|holder| get_start_vector_id(holder) <= idx)
        .checked_sub(1)
        .unwrap_or_else(|| {
            panic!(
                "idx {} too small for first vector id {}",
                idx,
                vector_holders.first().map_or(0, &get_start_vector_id)
            )
        });
    let holder = &vector_holders[holder_idx];
    let offset = idx - get_start_vector_id(holder);
    (holder_idx, offset)
}
