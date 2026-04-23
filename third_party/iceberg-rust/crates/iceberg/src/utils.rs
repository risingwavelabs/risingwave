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

use std::num::NonZeroUsize;

use crate::spec::{SnapshotRef, TableMetadataRef};

// Use a default value of 1 as the safest option.
// See https://doc.rust-lang.org/std/thread/fn.available_parallelism.html#limitations
// for more details.
const DEFAULT_PARALLELISM: usize = 1;

/// Uses [`std::thread::available_parallelism`] in order to
/// retrieve an estimate of the default amount of parallelism
/// that should be used. Note that [`std::thread::available_parallelism`]
/// returns a `Result` as it can fail, so here we use
/// a default value instead.
/// Note: we don't use a OnceCell or LazyCell here as there
/// are circumstances where the level of available
/// parallelism can change during the lifetime of an executing
/// process, but this should not be called in a hot loop.
pub(crate) fn available_parallelism() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or_else(|_err| {
        // Failed to get the level of parallelism.
        // TODO: log/trace when this fallback occurs.

        // Using a default value.
        NonZeroUsize::new(DEFAULT_PARALLELISM).unwrap()
    })
}

pub mod bin {
    use std::iter::Iterator;
    use std::marker::PhantomData;

    use itertools::Itertools;

    struct Bin<T> {
        bin_weight: u32,
        target_weight: u32,
        items: Vec<T>,
    }

    impl<T> Bin<T> {
        pub fn new(target_weight: u32) -> Self {
            Bin {
                bin_weight: 0,
                target_weight,
                items: Vec::new(),
            }
        }

        pub fn can_add(&self, weight: u32) -> bool {
            self.bin_weight + weight <= self.target_weight
        }

        pub fn add(&mut self, item: T, weight: u32) {
            self.bin_weight += weight;
            self.items.push(item);
        }

        pub fn into_vec(self) -> Vec<T> {
            self.items
        }
    }

    /// ListPacker help to pack item into bin of item. Each bin has close to
    /// target_weight.
    pub(crate) struct ListPacker<T> {
        target_weight: u32,
        _marker: PhantomData<T>,
    }

    impl<T> ListPacker<T> {
        pub fn new(target_weight: u32) -> Self {
            ListPacker {
                target_weight,
                _marker: PhantomData,
            }
        }

        pub fn pack<F>(&self, items: Vec<T>, weight_func: F) -> Vec<Vec<T>>
        where F: Fn(&T) -> u32 {
            let mut bins: Vec<Bin<T>> = vec![];
            for item in items {
                let cur_weight = weight_func(&item);
                let addable_bin =
                    if let Some(bin) = bins.iter_mut().find(|bin| bin.can_add(cur_weight)) {
                        bin
                    } else {
                        bins.push(Bin::new(self.target_weight));
                        bins.last_mut().unwrap()
                    };
                addable_bin.add(item, cur_weight);
            }

            bins.into_iter().map(|bin| bin.into_vec()).collect_vec()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_list_packer_basic_packing() {
            let packer = ListPacker::new(10);
            let items = vec![3, 4, 5, 6, 2, 1];

            let packed = packer.pack(items, |&x| x);

            assert_eq!(packed.len(), 3);
            assert!(packed[0].iter().sum::<u32>() == 10);
            assert!(packed[1].iter().sum::<u32>() == 5);
            assert!(packed[2].iter().sum::<u32>() == 6);
        }

        #[test]
        fn test_list_packer_with_complex_items() {
            #[derive(Debug, PartialEq)]
            struct Item {
                name: String,
                size: u32,
            }

            let packer = ListPacker::new(15);
            let items = vec![
                Item {
                    name: "A".to_string(),
                    size: 7,
                },
                Item {
                    name: "B".to_string(),
                    size: 8,
                },
                Item {
                    name: "C".to_string(),
                    size: 5,
                },
                Item {
                    name: "D".to_string(),
                    size: 6,
                },
            ];

            let packed = packer.pack(items, |item| item.size);

            assert_eq!(packed.len(), 2);
            assert!(packed[0].iter().map(|x| x.size).sum::<u32>() <= 15);
            assert!(packed[1].iter().map(|x| x.size).sum::<u32>() <= 15);
        }

        #[test]
        fn test_list_packer_single_large_item() {
            let packer = ListPacker::new(10);
            let items = vec![15, 5, 3];

            let packed = packer.pack(items, |&x| x);

            assert_eq!(packed.len(), 2);
            assert!(packed[0].contains(&15));
            assert!(packed[1].iter().sum::<u32>() <= 10);
        }

        #[test]
        fn test_list_packer_empty_input() {
            let packer = ListPacker::new(10);
            let items: Vec<u32> = vec![];

            let packed = packer.pack(items, |&x| x);

            assert_eq!(packed.len(), 0);
        }
    }
}

pub struct Ancestors {
    next: Option<SnapshotRef>,
    get_snapshot: Box<dyn Fn(i64) -> Option<SnapshotRef> + Send>,
}

impl Iterator for Ancestors {
    type Item = SnapshotRef;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.next.take()?;
        let result = snapshot.clone();
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(result)
    }
}

/// Iterate starting from `snapshot` (inclusive) to the root snapshot.
pub fn ancestors_of(
    table_metadata: &TableMetadataRef,
    snapshot: i64,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    if let Some(snapshot) = table_metadata.snapshot_by_id(snapshot) {
        let table_metadata = table_metadata.clone();
        Box::new(Ancestors {
            next: Some(snapshot.clone()),
            get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
        })
    } else {
        Box::new(std::iter::empty())
    }
}

/// Iterate starting from `snapshot` (inclusive) to `oldest_snapshot_id` (exclusive).
pub fn ancestors_between(
    table_metadata: &TableMetadataRef,
    latest_snapshot_id: i64,
    oldest_snapshot_id: Option<i64>,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    let Some(oldest_snapshot_id) = oldest_snapshot_id else {
        return Box::new(ancestors_of(table_metadata, latest_snapshot_id));
    };

    if latest_snapshot_id == oldest_snapshot_id {
        return Box::new(std::iter::empty());
    }

    Box::new(
        ancestors_of(table_metadata, latest_snapshot_id)
            .take_while(move |snapshot| snapshot.snapshot_id() != oldest_snapshot_id),
    )
}

use std::collections::HashSet;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::{self, StreamExt};

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{Manifest, ManifestFile, ManifestList, ManifestStatus, Snapshot};

pub(crate) const DEFAULT_DELETE_CONCURRENCY_LIMIT: usize = 10;
pub(crate) const DEFAULT_LOAD_CONCURRENCY_LIMIT: usize = 16;

/// Concurrently loads manifest lists for the given snapshots.
pub(crate) async fn load_manifest_lists(
    file_io: &FileIO,
    table_metadata: &TableMetadataRef,
    snapshots: Vec<SnapshotRef>,
    concurrency: usize,
) -> Result<Vec<(SnapshotRef, ManifestList)>> {
    let concurrency = concurrency.max(1);

    stream::iter(snapshots)
        .map(|snapshot| {
            let file_io = file_io.clone();
            let table_metadata = table_metadata.clone();
            async move {
                let manifest_list = snapshot
                    .load_manifest_list(&file_io, &table_metadata)
                    .await?;
                Ok((snapshot, manifest_list))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await
}

/// Concurrently loads manifests for the given manifest files.
pub(crate) async fn load_manifests(
    file_io: &FileIO,
    manifest_files: Vec<ManifestFile>,
    concurrency: usize,
) -> Result<Vec<(ManifestFile, Manifest)>> {
    let concurrency = concurrency.max(1);

    stream::iter(manifest_files)
        .map(|manifest_file| {
            let file_io = file_io.clone();
            async move {
                let manifest = manifest_file.load_manifest(&file_io).await?;
                Ok((manifest_file, manifest))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await
}

/// Strategy for cleaning up unreachable files after snapshot expiration.
pub struct ReachableFileCleanupStrategy {
    file_io: FileIO,
    delete_concurrency: usize,
    load_concurrency: usize,
}

impl ReachableFileCleanupStrategy {
    /// Creates a new cleanup strategy with default concurrency limits.
    pub fn new(file_io: FileIO) -> Self {
        Self {
            file_io,
            delete_concurrency: DEFAULT_DELETE_CONCURRENCY_LIMIT,
            load_concurrency: DEFAULT_LOAD_CONCURRENCY_LIMIT,
        }
    }

    fn collect_expired_snapshots<'a>(
        &'a self,
        before_expiration: &'a TableMetadataRef,
        after_expiration: &'a TableMetadataRef,
    ) -> (Vec<SnapshotRef>, HashSet<&'a str>) {
        let mut manifest_lists_to_delete: HashSet<&str> = HashSet::default();
        let mut expired_snapshots = Vec::default();

        for snapshot in before_expiration.snapshots() {
            if after_expiration
                .snapshot_by_id(snapshot.snapshot_id())
                .is_none()
            {
                expired_snapshots.push(snapshot.clone());
                manifest_lists_to_delete.insert(snapshot.manifest_list());
            }
        }

        (expired_snapshots, manifest_lists_to_delete)
    }

    /// Sets the concurrency limit for file deletion.
    pub fn with_delete_concurrency(mut self, limit: usize) -> Self {
        self.delete_concurrency = limit.max(1);
        self
    }

    /// Sets the concurrency limit for loading manifest lists and manifests.
    #[allow(dead_code)]
    pub fn with_load_concurrency(mut self, limit: usize) -> Self {
        self.load_concurrency = limit.max(1);
        self
    }

    /// Deletes files concurrently with the configured concurrency limit.
    async fn delete_files<I>(&self, paths: I) -> Result<()>
    where I: IntoIterator<Item = String> {
        stream::iter(paths)
            .map(|path| {
                let file_io = self.file_io.clone();
                async move { file_io.delete(&path).await }
            })
            .buffer_unordered(self.delete_concurrency)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// Cleans up files that became unreachable after snapshot expiration.
    ///
    /// Compares `before_expiration` and `after_expiration` metadata to identify expired
    /// snapshots, then deletes their unreferenced data files, manifests, and manifest lists.
    pub async fn clean_files(
        &self,
        before_expiration: &TableMetadataRef,
        after_expiration: &TableMetadataRef,
    ) -> Result<()> {
        let (expired_snapshots, manifest_lists_to_delete) =
            self.collect_expired_snapshots(before_expiration, after_expiration);

        let deletion_candidates = {
            let mut deletion_candidates = HashSet::default();
            let loaded = load_manifest_lists(
                &self.file_io,
                before_expiration,
                expired_snapshots,
                self.load_concurrency,
            )
            .await?;

            for (_, manifest_list) in loaded {
                for manifest_file in manifest_list.entries() {
                    deletion_candidates.insert(manifest_file.clone());
                }
            }
            deletion_candidates
        };

        if !deletion_candidates.is_empty() {
            let (manifests_to_delete, referenced_manifests) = self
                .prune_referenced_manifests(
                    after_expiration.snapshots(),
                    after_expiration,
                    deletion_candidates,
                )
                .await?;

            if !manifests_to_delete.is_empty() {
                let files_to_delete = self
                    .find_files_to_delete(&manifests_to_delete, &referenced_manifests)
                    .await?;

                self.delete_files(files_to_delete).await?;
                self.delete_files(manifests_to_delete.into_iter().map(|m| m.manifest_path))
                    .await?;
            }
        }

        let manifest_list_strings: Vec<String> = manifest_lists_to_delete
            .into_iter()
            .map(str::to_owned)
            .collect();
        self.delete_files(manifest_list_strings).await?;

        Ok(())
    }

    /// Separates manifests into deletable and still-referenced sets.
    async fn prune_referenced_manifests(
        &self,
        snapshots: impl Iterator<Item = &Arc<Snapshot>>,
        table_meta_data_ref: &TableMetadataRef,
        mut deletion_candidates: HashSet<ManifestFile>,
    ) -> Result<(HashSet<ManifestFile>, HashSet<ManifestFile>)> {
        let snapshots: Vec<_> = snapshots.cloned().collect();
        let loaded = load_manifest_lists(
            &self.file_io,
            table_meta_data_ref,
            snapshots,
            self.load_concurrency,
        )
        .await?;

        let mut referenced_manifests = HashSet::default();
        for (_, manifest_list) in loaded {
            for manifest_file in manifest_list.entries() {
                deletion_candidates.remove(manifest_file);
                referenced_manifests.insert(manifest_file.clone());
            }
        }

        Ok((deletion_candidates, referenced_manifests))
    }

    /// Finds data files that can be safely deleted.
    ///
    /// Collects all data file paths from the manifests being deleted, then removes
    /// any that are still actively referenced (status `Added` or `Existing`) by
    /// surviving manifests. Files with `Deleted` status in surviving manifests are
    /// tombstone entries and do not protect the underlying data files from deletion.
    async fn find_files_to_delete(
        &self,
        manifest_files: &HashSet<ManifestFile>,
        referenced_manifests: &HashSet<ManifestFile>,
    ) -> Result<HashSet<String>> {
        // Load manifests to delete concurrently
        let manifests_to_delete_vec: Vec<_> = manifest_files.iter().cloned().collect();
        let loaded_to_delete = load_manifests(
            &self.file_io,
            manifests_to_delete_vec,
            self.load_concurrency,
        )
        .await?;

        let mut files_to_delete = HashSet::default();
        for (_, manifest) in loaded_to_delete {
            for entry in manifest.entries() {
                files_to_delete.insert(entry.data_file().file_path().to_owned());
            }
        }

        if files_to_delete.is_empty() {
            return Ok(files_to_delete);
        }

        // Load referenced manifests concurrently
        let referenced_vec: Vec<_> = referenced_manifests.iter().cloned().collect();
        let loaded_referenced =
            load_manifests(&self.file_io, referenced_vec, self.load_concurrency).await?;

        // Only protect files that are actively referenced (Added or Existing).
        // Entries with Deleted status are tombstones — they indicate the file was
        // removed from the table and should not prevent its deletion from storage.
        for (_, manifest) in loaded_referenced {
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Deleted {
                    files_to_delete.remove(entry.data_file().file_path());
                }
            }
        }

        Ok(files_to_delete)
    }
}

#[cfg(test)]
mod cleanup_tests {
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIOBuilder;
    use crate::spec::TableMetadata;
    use crate::table::Table;

    #[cfg(test)]
    fn make_v2_table_with_multi_snapshot() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMultiSnapshot.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIOBuilder::new("memory").build().unwrap())
            .build()
            .unwrap()
    }

    #[cfg(test)]
    fn clone_without_snapshots(
        metadata: &TableMetadataRef,
        expired_ids: &HashSet<i64>,
    ) -> TableMetadataRef {
        let mut cloned = metadata.as_ref().clone();
        cloned.snapshots.retain(|id, _| !expired_ids.contains(id));
        cloned
            .snapshot_log
            .retain(|log| !expired_ids.contains(&log.snapshot_id));
        Arc::new(cloned)
    }

    #[test]
    fn test_cleanup_strategy_builder() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();

        // Test default concurrency limits
        let strategy = ReachableFileCleanupStrategy::new(file_io.clone());
        assert_eq!(
            strategy.delete_concurrency, DEFAULT_DELETE_CONCURRENCY_LIMIT,
            "Default delete concurrency limit should be {DEFAULT_DELETE_CONCURRENCY_LIMIT}",
        );
        assert_eq!(
            strategy.load_concurrency, DEFAULT_LOAD_CONCURRENCY_LIMIT,
            "Default load concurrency limit should be {DEFAULT_LOAD_CONCURRENCY_LIMIT}",
        );

        // Test custom delete concurrency limit
        let custom_limit = 20;
        let strategy = ReachableFileCleanupStrategy::new(file_io.clone())
            .with_delete_concurrency(custom_limit);
        assert_eq!(
            strategy.delete_concurrency, custom_limit,
            "Custom delete concurrency limit should be set correctly"
        );

        // Test custom load concurrency limit
        let strategy =
            ReachableFileCleanupStrategy::new(file_io).with_load_concurrency(custom_limit);
        assert_eq!(
            strategy.load_concurrency, custom_limit,
            "Custom load concurrency limit should be set correctly"
        );
    }

    /// Verifies that `Table::with_file_io` restores the original FileIO after
    /// a catalog commit strips S3 configuration.
    ///
    /// When a catalog's `update_table` returns a new `Table`, it uses the catalog's
    /// own `FileIO` which may lack properties like `s3.region`.
    /// `Transaction::commit` now calls `with_file_io` to restore the original FileIO
    /// so that `cleanup_expired_files_with_concurrency` works correctly.
    #[test]
    fn test_with_file_io_preserves_s3_region_after_catalog_commit() {
        use crate::io::S3_REGION;

        let s3_region = "us-east-1";

        let original_file_io = FileIOBuilder::new("memory")
            .with_prop(S3_REGION, s3_region)
            .build()
            .unwrap();

        let table_with_region = {
            let table = make_v2_table_with_multi_snapshot();
            Table::builder()
                .metadata(table.metadata().clone())
                .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
                .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
                .file_io(original_file_io.clone())
                .build()
                .unwrap()
        };

        let (_, props, _) = table_with_region
            .file_io()
            .clone()
            .into_builder()
            .into_parts();
        assert_eq!(
            props.get(S3_REGION).map(|s| s.as_str()),
            Some(s3_region),
            "Table built with original FileIO should carry the S3 region"
        );

        let catalog_file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_from_catalog = Table::builder()
            .metadata(table_with_region.metadata().clone())
            .metadata_location("s3://bucket/test/location/metadata/v2.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(catalog_file_io)
            .build()
            .unwrap();

        let (_, committed_props, _) = table_from_catalog
            .file_io()
            .clone()
            .into_builder()
            .into_parts();
        assert!(
            !committed_props.contains_key(S3_REGION),
            "Catalog-returned table's FileIO should lack the S3 region property"
        );

        let restored_table = table_from_catalog.with_file_io(original_file_io);

        let (_, restored_props, _) = restored_table.file_io().clone().into_builder().into_parts();
        assert_eq!(
            restored_props.get(S3_REGION).map(|s| s.as_str()),
            Some(s3_region),
            "After with_file_io the table's FileIO should carry the S3 region again"
        );
    }

    /// Helper to create a minimal DataFile with the given path.
    #[cfg(test)]
    fn make_data_file(path: &str) -> crate::spec::DataFile {
        use std::collections::HashMap;

        use crate::spec::{DataContentType, DataFile, DataFileFormat, Struct};

        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 1,
            file_size_in_bytes: 1024,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    /// Helper to write a manifest with the given entries to memory FileIO,
    /// returning the resulting `ManifestFile`.
    #[cfg(test)]
    async fn write_test_manifest(
        file_io: &crate::io::FileIO,
        path: &str,
        snapshot_id: i64,
        entries: Vec<(crate::spec::ManifestStatus, &str)>,
    ) -> crate::spec::ManifestFile {
        use crate::spec::{
            ManifestEntry, ManifestStatus, ManifestWriterBuilder, NestedField, PartitionSpec,
            PrimitiveType, Schema, Type,
        };

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        );
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        let output = file_io.new_output(path).unwrap();
        let mut writer =
            ManifestWriterBuilder::new(output, Some(snapshot_id), None, schema, partition_spec)
                .build_v2_data();

        for (status, file_path) in entries {
            let entry = ManifestEntry {
                status,
                snapshot_id: Some(snapshot_id),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                data_file: make_data_file(file_path),
            };
            match status {
                ManifestStatus::Added => writer.add_entry(entry).unwrap(),
                ManifestStatus::Deleted => writer.add_delete_entry(entry).unwrap(),
                ManifestStatus::Existing => writer.add_existing_entry(entry).unwrap(),
            }
        }

        writer.write_manifest_file().await.unwrap()
    }

    /// Verifies that `find_files_to_delete` does not protect data files whose
    /// entries in surviving manifests have `ManifestStatus::Deleted` (tombstones).
    ///
    /// After compaction, replaced data files appear as `Deleted` entries in the
    /// current snapshot's manifests.  Before the fix, these tombstone entries
    /// incorrectly prevented the underlying data files from being cleaned up
    /// when the old snapshot expired.
    #[tokio::test]
    async fn test_deleted_status_entries_do_not_protect_files_from_cleanup() {
        use crate::spec::ManifestStatus;

        let file_io = FileIOBuilder::new("memory").build().unwrap();

        // ---- set up manifests ----
        // Manifest belonging to an expired snapshot.
        // Contains file_a (replaced by compaction) and file_b (still live).
        let expired_manifest = write_test_manifest(
            &file_io,
            "memory://manifests/expired.avro",
            1, // snapshot_id
            vec![
                (ManifestStatus::Added, "memory://data/file_a.parquet"),
                (ManifestStatus::Added, "memory://data/file_b.parquet"),
            ],
        )
        .await;

        // Manifest still referenced by the current (surviving) snapshot.
        // file_a has Deleted status (tombstone from compaction) — must NOT
        // protect file_a from deletion.
        // file_b has Existing status — must protect file_b from deletion.
        let surviving_manifest = write_test_manifest(
            &file_io,
            "memory://manifests/surviving.avro",
            2, // snapshot_id
            vec![
                (ManifestStatus::Deleted, "memory://data/file_a.parquet"),
                (ManifestStatus::Existing, "memory://data/file_b.parquet"),
            ],
        )
        .await;

        // ---- call find_files_to_delete ----
        let manifests_to_delete: HashSet<_> = [expired_manifest].into_iter().collect();
        let referenced_manifests: HashSet<_> = [surviving_manifest].into_iter().collect();

        let strategy = ReachableFileCleanupStrategy::new(file_io);
        let files_to_delete = strategy
            .find_files_to_delete(&manifests_to_delete, &referenced_manifests)
            .await
            .expect("find_files_to_delete should succeed");

        // file_a should be deleted: the tombstone (Deleted status) in the
        // surviving manifest must NOT prevent its removal.
        assert!(
            files_to_delete.contains("memory://data/file_a.parquet"),
            "file_a should be marked for deletion because the Deleted tombstone \
             in the surviving manifest must not protect it"
        );

        // file_b should NOT be deleted: it has Existing status in the
        // surviving manifest, so it is still actively referenced.
        assert!(
            !files_to_delete.contains("memory://data/file_b.parquet"),
            "file_b should be protected from deletion because it has Existing \
             status in the surviving manifest"
        );

        assert_eq!(
            files_to_delete.len(),
            1,
            "Exactly one file (file_a) should be marked for deletion"
        );
    }

    /// Verifies that when a surviving manifest references a file with `Added`
    /// status, that file is protected from deletion — even if an expired
    /// manifest also references it.
    #[tokio::test]
    async fn test_added_status_entries_protect_files_from_cleanup() {
        use crate::spec::ManifestStatus;

        let file_io = FileIOBuilder::new("memory").build().unwrap();

        let expired_manifest =
            write_test_manifest(&file_io, "memory://manifests/expired.avro", 1, vec![(
                ManifestStatus::Added,
                "memory://data/file_c.parquet",
            )])
            .await;

        // The same file appears as Added in a surviving manifest.
        let surviving_manifest =
            write_test_manifest(&file_io, "memory://manifests/surviving.avro", 2, vec![(
                ManifestStatus::Added,
                "memory://data/file_c.parquet",
            )])
            .await;

        let manifests_to_delete: HashSet<_> = [expired_manifest].into_iter().collect();
        let referenced_manifests: HashSet<_> = [surviving_manifest].into_iter().collect();

        let strategy = ReachableFileCleanupStrategy::new(file_io);
        let files_to_delete = strategy
            .find_files_to_delete(&manifests_to_delete, &referenced_manifests)
            .await
            .expect("find_files_to_delete should succeed");

        assert!(
            files_to_delete.is_empty(),
            "No files should be deleted when the surviving manifest has the file as Added"
        );
    }

    #[test]
    fn test_expired_snapshot_detection_scenarios() {
        let table = make_v2_table_with_multi_snapshot();
        let before_metadata = table.metadata_ref();
        let strategy =
            ReachableFileCleanupStrategy::new(FileIOBuilder::new("memory").build().unwrap());

        let mut all_snapshot_ids: Vec<i64> = before_metadata
            .snapshots()
            .map(|s| s.snapshot_id())
            .collect();
        all_snapshot_ids.sort_unstable();
        assert!(
            all_snapshot_ids.len() > 2,
            "Fixture should provide multiple snapshots to exercise expiration logic"
        );

        let expired_ids: HashSet<i64> = all_snapshot_ids.iter().take(2).copied().collect();
        let after_with_expired = clone_without_snapshots(&before_metadata, &expired_ids);
        assert_eq!(
            after_with_expired.snapshots().count(),
            all_snapshot_ids.len() - expired_ids.len(),
            "After-metadata should retain the non-expired snapshots only"
        );

        let (expired_snapshots, manifest_lists_to_delete) =
            strategy.collect_expired_snapshots(&before_metadata, &after_with_expired);

        let expired_ids_found: HashSet<_> =
            expired_snapshots.iter().map(|s| s.snapshot_id()).collect();

        assert_eq!(
            expired_ids_found.len(),
            expired_ids.len(),
            "The number of expired snapshots should match the removed snapshots"
        );
        assert_eq!(
            manifest_lists_to_delete.len(),
            expired_ids.len(),
            "Each expired snapshot must contribute one manifest list"
        );
        assert_eq!(
            expired_ids_found, expired_ids,
            "Expired snapshot IDs should be identified precisely"
        );
        for snapshot in &expired_snapshots {
            assert!(
                manifest_lists_to_delete.contains(snapshot.manifest_list()),
                "Expired snapshots must provide their manifest list for deletion"
            );
            assert!(
                !snapshot.manifest_list().is_empty(),
                "Manifest list path should not be empty"
            );
        }

        let remaining_ids: HashSet<_> = all_snapshot_ids
            .iter()
            .copied()
            .filter(|id| !expired_ids.contains(id))
            .collect();
        assert!(
            remaining_ids.is_disjoint(&expired_ids_found),
            "Snapshots retained in after-metadata must not be marked expired"
        );

        let (no_expired, no_manifest_lists) =
            strategy.collect_expired_snapshots(&before_metadata, &before_metadata);
        assert!(
            no_expired.is_empty() && no_manifest_lists.is_empty(),
            "When after-metadata keeps all snapshots, expired collections should be empty"
        );
    }
}
