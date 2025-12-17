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

use std::path::Path;

use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use super::BatchPosixFsProperties;
use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::batch::BatchSourceSplit;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SplitEnumerator,
    SplitId, SplitMetaData, SplitReader,
};

/// Batch Posix fs source for refreshable tables. (For testing only)
/// Unlike regular `posix_fs`, this connector only lists files on demand (during refresh),
/// not continuously. This makes it suitable for refreshable table functionality.
///
/// Split representing a single file to be read once
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct BatchPosixFsSplit {
    /// For batch posix fs, this is always the root directory. The reader will
    /// scan all files in this directory.
    pub file_path: String,
    /// A unique identifier for the split, typically including a timestamp to force refresh.
    pub split_id: SplitId,
    /// Whether this split has finished reading all data (used for batch sources)
    /// See [`BatchSourceSplit`] for details about recovery.
    #[serde(skip)]
    pub finished: bool,
}

impl SplitMetaData for BatchPosixFsSplit {
    fn id(&self) -> SplitId {
        self.split_id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e).into())
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // Batch source doesn't use offsets - each file is read completely once
        Ok(())
    }
}

impl BatchSourceSplit for BatchPosixFsSplit {
    fn finished(&self) -> bool {
        self.finished
    }

    fn finish(&mut self) {
        self.finished = true;
    }

    fn refresh(&mut self) {
        self.finished = false;
    }
}

impl BatchPosixFsSplit {
    pub fn new(file_path: String, split_id: SplitId) -> Self {
        Self {
            file_path,
            split_id,
            finished: false,
        }
    }

    pub fn mark_finished(&mut self) {
        self.finished = true;
    }
}

/// Enumerator for batch posix fs source
#[derive(Debug)]
pub struct BatchPosixFsEnumerator {
    properties: BatchPosixFsProperties,
}

#[async_trait]
impl SplitEnumerator for BatchPosixFsEnumerator {
    type Properties = BatchPosixFsProperties;
    type Split = BatchPosixFsSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self { properties })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<BatchPosixFsSplit>> {
        // dummy list, just return one split
        let root_path = Path::new(&self.properties.root);

        if !root_path.exists() {
            return Err(anyhow!("Root directory does not exist: {}", self.properties.root).into());
        }

        // For batch source, we return exactly one split representing all files to be processed.
        Ok(vec![BatchPosixFsSplit::new(
            self.properties.root.clone(), // file_path is the root
            "114514".into(),              // split_id does not matter
        )])
    }
}

/// Reader for batch posix fs source
#[derive(Debug)]
pub struct BatchPosixFsReader {}

#[async_trait]
impl SplitReader for BatchPosixFsReader {
    type Properties = BatchPosixFsProperties;
    type Split = BatchPosixFsSplit;

    async fn new(
        _properties: Self::Properties,
        _splits: Vec<Self::Split>,
        _parser_config: ParserConfig,
        _source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        return Err(anyhow!("BatchPosixFsReader should not be used").into());
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        unreachable!(
            "BatchPosixFsReader should not hit this branch. refer to `batch_posix_fs_list.rs`, `batch_posix_fs_fetch.rs`"
        );
    }
}
