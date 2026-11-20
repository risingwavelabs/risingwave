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

use bytes::Bytes;
use risingwave_common::array::VectorRef;

use super::object_index_writer::HummockObjectIndexWriter;
use crate::error::StorageResult;
use crate::hummock::vector::writer::VectorWriterImpl;
use crate::store::*;

pub struct HummockVectorWriter(HummockObjectIndexWriter<VectorWriterImpl>);

impl HummockVectorWriter {
    pub(super) fn new(inner: HummockObjectIndexWriter<VectorWriterImpl>) -> Self {
        Self(inner)
    }
}

impl StateStoreWriteEpochControl for HummockVectorWriter {
    async fn init(&mut self, opts: InitOptions) -> StorageResult<()> {
        self.0.init(opts).await
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
        self.0.seal_current_epoch(next_epoch, opts)
    }

    async fn flush(&mut self) -> StorageResult<usize> {
        self.0.flush().await
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        self.0.try_flush().await
    }
}

impl StateStoreWriteVector for HummockVectorWriter {
    fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> StorageResult<()> {
        Ok(self.0.writer_impl().insert(vec, info)?)
    }
}
