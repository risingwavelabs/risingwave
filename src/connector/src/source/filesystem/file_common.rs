// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::source::{BoxFsSourceStream, SplitId, SplitMetaData};

///  [`FsSplit`] Describes a file or a split of a file. A file is a generic concept,
/// and can be a local file, a distributed file system, or am object in S3 bucket.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FsSplit {
    pub name: String,
    pub offset: usize,
    pub size: usize,
}

impl SplitMetaData for FsSplit {
    fn id(&self) -> SplitId {
        self.name.as_str().into()
    }

    fn encode_to_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

impl FsSplit {
    pub fn new(name: String, start: usize, size: usize) -> Self {
        Self {
            name,
            offset: start,
            size,
        }
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        let offset = start_offset.parse().unwrap();
        Self::new(self.name.clone(), offset, self.size)
    }

    pub fn clone_with_offset(&self, offset: usize) -> Self {
        Self::new(self.name.clone(), offset, self.size)
    }
}

/// [`FsSplitReader`] is an abstraction of the external fs connector read interface,
/// used to read messages from the outside and transform them into source-oriented
/// [`FsSourceMessage`], in order to improve throughput, it is recommended to return a batch of
/// messages at a time.
#[async_trait]
pub trait FsSplitReader: Sized {
    type Properties;

    async fn new(properties: Self::Properties, splits: Vec<FsSplit>) -> Result<Self>;

    fn into_stream(self) -> BoxFsSourceStream;
}
