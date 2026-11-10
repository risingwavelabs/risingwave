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

use bytes::Bytes;

use super::object_index_writer::HummockObjectIndexWriter;
use crate::error::StorageResult;
use crate::hummock::full_text::writer::FullTextIndexWriter;
use crate::store::*;

pub struct HummockFullTextWriter(HummockObjectIndexWriter<FullTextIndexWriter>);

impl HummockFullTextWriter {
    pub(super) fn new(inner: HummockObjectIndexWriter<FullTextIndexWriter>) -> Self {
        Self(inner)
    }
}

impl StateStoreWriteEpochControl for HummockFullTextWriter {
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

impl StateStoreWriteFullText for HummockFullTextWriter {
    fn set_maintenance_enabled(&mut self, enabled: bool) {
        self.0.writer_impl().set_maintenance_enabled(enabled);
    }

    fn insert_document(&mut self, key: Bytes, tokens: &[String]) -> StorageResult<()> {
        Ok(self.0.writer_impl().insert_document(key, tokens)?)
    }

    fn delete_document(&mut self, key: Bytes) -> StorageResult<()> {
        Ok(self.0.writer_impl().delete_document(key)?)
    }
}
