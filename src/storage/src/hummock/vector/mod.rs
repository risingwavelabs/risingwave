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

use crate::hummock::vector::file::{VectorBlock, VectorBlockBuilder};
use crate::hummock::{HummockResult, SstableStoreRef, VectorBlockHolder};
use crate::vector::VectorRef;
use crate::vector::hnsw::{VectorAccessor, VectorStore};

pub(crate) mod file;
pub mod writer;

pub struct VectorBlockAccessor<'a> {
    block: &'a VectorBlockHolder,
    idx: usize,
}

impl VectorAccessor for VectorBlockAccessor<'_> {
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
    BloclHolder(VectorBlockAccessor<'a>),
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

struct VectorFileBlock {
    block: VectorBlockHolder,
    start_vector_id: usize,
    vector_count: usize,
}

pub struct VectorFileBlockStore {
    sstable_store: SstableStoreRef,
    blocks: Vec<VectorFileBlock>,
}

impl VectorFileBlockStore {
    pub fn get_vector_block(&self, idx: usize) -> VectorBlockAccessor<'_> {
        let (idx, offset) = search_vector(&self.blocks, idx, |block| block.start_vector_id);
        VectorBlockAccessor {
            block: &self.blocks[idx].block,
            idx: offset,
        }
    }

    pub async fn new(
        sstable_store: SstableStoreRef,
        files: impl IntoIterator<Item = &VectorFileInfo>,
    ) -> HummockResult<Self> {
        let mut ret = Self {
            sstable_store,
            blocks: Vec::new(),
        };
        for file in files {
            ret.add(file).await?;
        }
        Ok(ret)
    }

    pub async fn add(&mut self, file: &VectorFileInfo) -> HummockResult<()> {
        let meta = self.sstable_store.get_vector_file_meta(file).await?;
        for (idx, block) in meta.block_metas.iter().enumerate() {
            if let Some(prev_block) = self.blocks.last() {
                assert_eq!(
                    prev_block.start_vector_id + prev_block.vector_count,
                    block.start_vector_id
                );
            }
            let block_holder = self
                .sstable_store
                .get_vector_block(file, idx, block)
                .await?;
            self.blocks.push(VectorFileBlock {
                block: block_holder,
                start_vector_id: block.start_vector_id,
                vector_count: block.vector_count,
            });
        }

        Ok(())
    }
}

impl VectorStore for VectorFileBlockStore {
    type Accessor<'a>
        = VectorBlockAccessor<'a>
    where
        Self: 'a;

    fn get_vector(&self, idx: usize) -> Self::Accessor<'_> {
        self.get_vector_block(idx)
    }
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
