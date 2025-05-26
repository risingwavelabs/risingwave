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
use crate::hummock::{HummockError, HummockResult, SstableStoreRef, VectorBlockHolder};
use crate::vector::VectorRef;
use crate::vector::hnsw::{VectorAccessor, VectorStore};

pub(crate) mod file;
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
    BloclHolder(VectorBlockAccessor),
}

impl VectorAccessor for EnumVectorAccessor<'_> {
    fn vec_ref(&self) -> VectorRef<'_> {
        match self {
            EnumVectorAccessor::Builder(builder, offset) => builder.vec_ref(*offset),
            EnumVectorAccessor::BlockRef(block, offset) => block.vec_ref(*offset),
            EnumVectorAccessor::BloclHolder(accessor) => accessor.vec_ref(),
        }
    }

    fn info(&self) -> &[u8] {
        match self {
            EnumVectorAccessor::Builder(builder, offset) => builder.info(*offset),
            EnumVectorAccessor::BlockRef(block, offset) => block.info(*offset),
            EnumVectorAccessor::BloclHolder(accessor) => accessor.info(),
        }
    }
}

pub async fn get_vector_block(
    sstable_store: &SstableStoreRef,
    files: &[VectorFileInfo],
    idx: usize,
) -> HummockResult<VectorBlockAccessor> {
    let vector_file = search_vector_files(files, idx)?;
    let meta = sstable_store.get_vector_file_meta(vector_file).await?;
    let (block_meta, block_idx, offset) = search_blocks(&meta.block_metas, idx)?;
    let block = sstable_store
        .get_vector_block(vector_file, block_idx, block_meta)
        .await?;
    Ok(VectorBlockAccessor { block, idx: offset })
}

pub struct FileVectorStore {
    vector_files: Vec<VectorFileInfo>,
    sstable_store: SstableStoreRef,
}

impl FileVectorStore {
    pub fn new(vector_files: Vec<VectorFileInfo>, sstable_store: SstableStoreRef) -> Self {
        Self {
            vector_files,
            sstable_store,
        }
    }
}

impl VectorStore for FileVectorStore {
    type Accessor<'a>
        = VectorBlockAccessor
    where
        Self: 'a;

    async fn get_vector(&self, idx: usize) -> HummockResult<Self::Accessor<'_>> {
        get_vector_block(&self.sstable_store, &self.vector_files, idx).await
    }
}

fn search_vector_files(files: &[VectorFileInfo], idx: usize) -> HummockResult<&VectorFileInfo> {
    let (file_idx, _) = search_vector(
        files,
        idx,
        |file| file.start_vector_id,
        |file| file.vector_count,
    )?;
    Ok(&files[file_idx])
}

fn search_blocks(
    blocks: &[VectorBlockMeta],
    idx: usize,
) -> HummockResult<(&VectorBlockMeta, usize, usize)> {
    let (block_idx, offset) = search_vector(
        blocks,
        idx,
        |block| block.start_vector_id,
        |block| block.vector_count,
    )?;
    Ok((&blocks[block_idx], block_idx, offset))
}

/// return (`holder_idx`, `offset` inside holder)
pub fn search_vector<T: std::fmt::Debug>(
    vector_holders: &[T],
    idx: usize,
    get_start_vector_id: impl Fn(&T) -> usize,
    get_vector_count: impl Fn(&T) -> usize,
) -> HummockResult<(usize, usize)> {
    let holder_idx = vector_holders
        .partition_point(|holder| get_start_vector_id(holder) <= idx)
        .checked_sub(1)
        .ok_or_else(|| {
            HummockError::other(format!(
                "idx {} too small for first vector id {}",
                idx,
                vector_holders.first().map_or(0, &get_start_vector_id)
            ))
        })?;
    let holder = &vector_holders[holder_idx];
    let offset = idx - get_start_vector_id(holder);
    if offset >= get_vector_count(holder) {
        return Err(HummockError::other(format!(
            "idx {} out of range for {:?}",
            idx, holder
        )));
    }
    Ok((holder_idx, offset))
}
