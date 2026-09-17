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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::Result;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, ListEntry, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use serde::{Deserialize, Serialize};

/// Count real read/stat/reader requests below `FileIO`, including attempted missing-file reads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CountingIo {
    #[serde(skip)]
    pub metadata_reads: Arc<AtomicUsize>,
    #[serde(skip)]
    pub row_reads: Arc<AtomicUsize>,
}

impl CountingIo {
    fn read_request(&self, path: &str) {
        let counter = if path.ends_with(".avro") {
            &self.metadata_reads
        } else {
            &self.row_reads
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[typetag::serde]
impl StorageFactory for CountingIo {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for CountingIo {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.read_request(path);
        LocalFsStorage.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.read_request(path);
        LocalFsStorage.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_request(path);
        LocalFsStorage.read(path).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.read_request(path);
        LocalFsStorage.reader(path).await
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        LocalFsStorage.write(path, bytes).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        LocalFsStorage.writer(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        LocalFsStorage.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        LocalFsStorage.delete_prefix(path).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        LocalFsStorage.delete_stream(paths).await
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        self.read_request(path);
        LocalFsStorage.list(path, recursive).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}
