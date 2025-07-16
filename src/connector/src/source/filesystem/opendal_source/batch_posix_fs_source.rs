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

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures_async_stream::try_stream;
use glob;
use risingwave_common::array::StreamChunk;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use tokio::fs;

use super::BatchPosixFsProperties;
use crate::error::ConnectorResult;
use crate::parser::{ByteStreamSourceParserImpl, ParserConfig};
use crate::source::batch::BatchSourceSplit;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceMessage,
    SourceMeta, SplitEnumerator, SplitId, SplitMetaData, SplitReader,
};

// Batch Posix fs source for refreshable tables.
// Unlike regular posix_fs, this connector only lists files on demand (during refresh),
// not continuously. This makes it suitable for refreshable table functionality.
//
// For a single-CN cluster, the behavior is well-defined. It will read from the local file system.
// For a multi-CN cluster, each CN will read from its own local file system under the given directory.

/// Split representing a single file to be read once
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct BatchPosixFsSplit {
    /// For batch posix fs, this is always the root directory. The reader will
    /// scan all files in this directory.
    pub file_path: String,
    /// A unique identifier for the split, typically including a timestamp to force refresh.
    pub split_id: SplitId,
    /// Whether this split has finished reading all data (used for batch sources)
    #[serde(default)]
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
        let root_path = Path::new(&self.properties.root);

        if !root_path.exists() {
            return Err(anyhow!("Root directory does not exist: {}", self.properties.root).into());
        }

        // For batch source, we return exactly one split representing all files to be processed.
        Ok(vec![BatchPosixFsSplit::new(
            self.properties.root.clone(), // file_path is the root
            "114514".into(),              // split_id is unique
        )])
    }
}

/// Reader for batch posix fs source
#[derive(Debug)]
pub struct BatchPosixFsReader {
    properties: BatchPosixFsProperties,
    splits: Vec<BatchPosixFsSplit>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for BatchPosixFsReader {
    type Properties = BatchPosixFsProperties;
    type Split = BatchPosixFsSplit;

    async fn new(
        properties: Self::Properties,
        splits: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            properties,
            splits,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        self.into_stream_inner()
    }
}

impl BatchPosixFsReader {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in &self.splits {
            let files = self.collect_files_for_split(split).await?;
            tracing::debug!(?files, ?split, "BatchPosixFsReader: colleted files");

            for file_path in files {
                let full_path = Path::new(&self.properties.root).join(&file_path);

                // Read the entire file at once
                let content = fs::read(&full_path)
                    .await
                    .with_context(|| format!("Failed to read file: {}", full_path.display()))?;

                if content.is_empty() {
                    continue;
                }

                // Create a single message with the entire file content
                let message = SourceMessage {
                    key: None,
                    payload: Some(content),
                    offset: "0".to_owned(), // Single read, no offset needed
                    split_id: split.id(),
                    meta: SourceMeta::Empty,
                };

                // Parse the content
                let parser = ByteStreamSourceParserImpl::create(
                    self.parser_config.clone(),
                    self.source_ctx.clone(),
                )
                .await?;

                let chunk_stream = parser
                    .parse_stream(Box::pin(futures::stream::once(async { Ok(vec![message]) })));

                #[for_await]
                for chunk in chunk_stream {
                    yield chunk?;
                }
            }
        }

        // Log completion for debugging
        tracing::info!("BatchPosixFs has finished reading all files");
    }

    async fn collect_files_for_split(
        &self,
        _split: &BatchPosixFsSplit,
    ) -> ConnectorResult<Vec<String>> {
        let root_path = Path::new(&self.properties.root);
        let mut files = Vec::new();

        let pattern = self
            .properties
            .match_pattern
            .as_ref()
            .map(|p| glob::Pattern::new(p))
            .transpose()
            .with_context(|| {
                format!("Invalid match_pattern: {:?}", self.properties.match_pattern)
            })?;

        self.collect_files_recursive(root_path, root_path, &pattern, &mut files)
            .await?;
        Ok(files)
    }

    fn collect_files_recursive<'a>(
        &'a self,
        current_dir: &'a Path,
        root_path: &'a Path,
        pattern: &'a Option<glob::Pattern>,
        files: &'a mut Vec<String>,
    ) -> BoxFuture<'a, ConnectorResult<()>> {
        Box::pin(async move {
            let mut entries = fs::read_dir(current_dir)
                .await
                .with_context(|| format!("Failed to read directory: {}", current_dir.display()))?;

            while let Some(entry) = entries.next_entry().await.with_context(|| {
                format!(
                    "Failed to read directory entry in: {}",
                    current_dir.display()
                )
            })? {
                let path = entry.path();

                if path.is_dir() {
                    // Recursively process subdirectories
                    self.collect_files_recursive(&path, root_path, pattern, files)
                        .await?;
                } else if path.is_file() {
                    let relative_path = path.strip_prefix(root_path).with_context(|| {
                        format!("Failed to get relative path for: {}", path.display())
                    })?;

                    let relative_path_str = relative_path.to_string_lossy().to_string();

                    // Check if file matches the pattern (if specified)
                    if let Some(pattern) = pattern {
                        if pattern.matches(&relative_path_str) {
                            files.push(relative_path_str);
                        }
                    } else {
                        files.push(relative_path_str);
                    }
                }
            }

            Ok(())
        })
    }
}
