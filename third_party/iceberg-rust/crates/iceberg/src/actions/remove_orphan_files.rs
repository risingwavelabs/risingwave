// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Remove orphan files action.
//!
//! Removes files under the table location that are not referenced by any snapshot
//! or table metadata. Files without a `last_modified` timestamp are skipped to
//! avoid deleting in-progress writes.

use std::collections::HashSet;
use std::time::Duration;

use futures::TryStreamExt;
use futures::stream::{self, StreamExt};

use crate::Result;
use crate::table::Table;
use crate::utils::{load_manifest_lists, load_manifests};

/// Default time offset for orphan file deletion threshold (1 day in milliseconds).
const DEFAULT_OLDER_THAN_MS: i64 = 7 * 24 * 60 * 60 * 1000; // 7 days

/// Default concurrency limit for loading manifests and manifest lists.
const DEFAULT_LOAD_CONCURRENCY: usize = 16;

/// Default concurrency limit for file deletion.
const DEFAULT_DELETE_CONCURRENCY: usize = 10;

/// Action to delete orphan files from a table's location.
///
/// ```ignore
/// let orphan_files = RemoveOrphanFilesAction::new(table)
///     .older_than(Duration::from_secs(3600))
///     .dry_run(true)
///     .execute()
///     .await?;
/// ```
pub struct RemoveOrphanFilesAction {
    table: Table,
    older_than_ms: i64,
    dry_run: bool,
    load_concurrency: usize,
    delete_concurrency: usize,
}

impl RemoveOrphanFilesAction {
    /// Creates a new delete orphan files action for the given table.
    pub fn new(table: Table) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64;

        Self {
            table,
            older_than_ms: now_ms - DEFAULT_OLDER_THAN_MS,
            dry_run: false,
            load_concurrency: DEFAULT_LOAD_CONCURRENCY,
            delete_concurrency: DEFAULT_DELETE_CONCURRENCY,
        }
    }

    /// Sets the timestamp threshold (milliseconds since epoch).
    pub fn older_than_ms(mut self, timestamp_ms: i64) -> Self {
        self.older_than_ms = timestamp_ms;
        self
    }

    /// Sets the older-than duration relative to now.
    pub fn older_than(mut self, duration: Duration) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64;
        self.older_than_ms = now_ms - duration.as_millis() as i64;
        self
    }

    /// Enables dry run mode - identifies orphan files without deleting them.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Sets the concurrency limit for loading manifest lists and manifests.
    pub fn load_concurrency(mut self, concurrency: usize) -> Self {
        self.load_concurrency = concurrency.max(1);
        self
    }

    /// Sets the concurrency limit for delete operations.
    pub fn delete_concurrency(mut self, concurrency: usize) -> Self {
        self.delete_concurrency = concurrency.max(1);
        self
    }

    /// Executes the action, returning the list of orphan files.
    pub async fn execute(self) -> Result<Vec<String>> {
        let file_io = self.table.file_io();
        let location = self.table.metadata().location();

        // Build the set of reachable files
        let reachable_files = self.collect_reachable_files().await?;

        // Stream all files under the table location and filter for orphans
        let file_stream = file_io.list(&location, true).await?;
        let older_than_ms = self.older_than_ms;

        // Find orphan files: not reachable, not a directory, and older than threshold
        let orphan_files: Vec<String> = file_stream
            .try_filter_map(|entry| {
                let is_orphan =
                    // Must be a file (not directory)
                    !entry.metadata.is_dir
                    // Must not be reachable
                    && !reachable_files.contains(&entry.path)
                    // Must have a timestamp and be older than threshold
                    // (files without timestamp are skipped to protect in-progress writes)
                    && entry.metadata.last_modified_ms.is_some_and(|ts| ts < older_than_ms);

                async move { Ok(is_orphan.then_some(entry.path)) }
            })
            .try_collect()
            .await?;

        if self.dry_run {
            return Ok(orphan_files);
        }

        // Remove orphan files concurrently.
        // Clone paths into owned Strings so each async task owns its data,
        // making the resulting future Send-safe (avoids HRTB lifetime issues
        // with borrowed references across await points).
        let file_io = file_io.clone();
        stream::iter(orphan_files.clone())
            .map(|path| {
                let file_io = file_io.clone();
                async move { file_io.delete(&path).await }
            })
            .buffer_unordered(self.delete_concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(orphan_files)
    }

    /// Collects all files reachable from table metadata and snapshots.
    async fn collect_reachable_files(&self) -> Result<HashSet<String>> {
        let mut reachable = HashSet::new();

        // 1. Collect metadata files
        self.collect_metadata_files(&mut reachable);

        // 2. Collect files from all snapshots
        self.collect_snapshot_files(&mut reachable).await?;

        Ok(reachable)
    }

    /// Collects metadata files (current, historical, statistics).
    fn collect_metadata_files(&self, reachable: &mut HashSet<String>) {
        let table_metadata = self.table.metadata_ref();

        // Current metadata file
        if let Some(metadata_location) = self.table.metadata_location() {
            reachable.insert(metadata_location.to_string());
        }

        // Historical metadata files
        reachable.extend(
            table_metadata
                .metadata_log()
                .iter()
                .map(|e| e.metadata_file.clone()),
        );

        // Statistics files
        reachable.extend(
            table_metadata
                .statistics_iter()
                .map(|s| s.statistics_path.clone()),
        );

        // Partition statistics files
        reachable.extend(
            table_metadata
                .partition_statistics_iter()
                .map(|s| s.statistics_path.clone()),
        );
    }

    /// Collects manifest lists, manifests, and content files from snapshots.
    async fn collect_snapshot_files(&self, reachable: &mut HashSet<String>) -> Result<()> {
        let file_io = self.table.file_io();
        let table_metadata = self.table.metadata_ref();

        let snapshots: Vec<_> = table_metadata.snapshots().cloned().collect();

        // Load manifest lists concurrently using shared loader
        let loaded_lists =
            load_manifest_lists(file_io, &table_metadata, snapshots, self.load_concurrency).await?;

        // Collect manifest list paths, manifest files, and deduplicate for loading
        let mut unique_manifest_files = Vec::new();
        for (snapshot, manifest_list) in loaded_lists {
            // Record manifest list path as reachable
            let manifest_list_path = snapshot.manifest_list();
            if !manifest_list_path.is_empty() {
                reachable.insert(manifest_list_path.to_string());
            }

            for manifest_file in manifest_list.entries() {
                // Only load each manifest once (reachable set handles deduplication)
                if reachable.insert(manifest_file.manifest_path.clone()) {
                    unique_manifest_files.push(manifest_file.clone());
                }
            }
        }

        // Load manifests concurrently using shared loader and collect content files
        let loaded_manifests =
            load_manifests(file_io, unique_manifest_files, self.load_concurrency).await?;

        for (_, manifest) in loaded_manifests {
            for entry in manifest.entries() {
                reachable.insert(entry.data_file().file_path().to_string());
            }
        }

        Ok(())
    }
}
