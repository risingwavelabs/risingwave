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

use crate::hummock::vector::block::VectorBlock;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::vector::VectorRef;
use crate::vector::hnsw::{VectorAccessor, VectorStore};

pub(crate) mod block;
pub(crate) mod writer;

pub struct VectorBlockAccessor {
    block: VectorBlock,
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
        let (vector_file, offset) = search_vector_files(&self.vector_files, idx)?;
        let block = self.sstable_store.get_vector_block(vector_file).await?;
        Ok(VectorBlockAccessor { block, idx: offset })
    }
}

pub fn search_vector_files(
    files: &[VectorFileInfo],
    idx: usize,
) -> HummockResult<(&VectorFileInfo, usize)> {
    let file_idx = files
        .partition_point(|file_info| file_info.start_vector_id <= idx)
        .checked_sub(1)
        .ok_or_else(|| {
            HummockError::other(format!(
                "idx {} too small for first vector id {}",
                idx,
                files.first().map_or(0, |f| f.start_vector_id)
            ))
        })?;
    let vector_file = &files[file_idx];
    let offset = idx - vector_file.start_vector_id;
    if offset >= vector_file.vector_count {
        return Err(HummockError::other(format!(
            "idx {} out of range for file {}",
            idx, vector_file.object_id
        )));
    }
    Ok((vector_file, offset))
}
