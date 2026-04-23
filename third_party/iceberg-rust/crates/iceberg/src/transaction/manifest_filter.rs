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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use uuid::Uuid;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, PartitionSpec, Schema,
};
use crate::transaction::snapshot::new_manifest_path;
use crate::{Error, ErrorKind};

/// Context for creating manifest writers during filtering operations
pub struct ManifestWriterContext {
    metadata_location: String,
    meta_root_path: String,
    commit_uuid: Uuid,
    manifest_counter: Arc<AtomicU64>,
    format_version: FormatVersion,
    snapshot_id: i64,
    file_io: FileIO,
    key_metadata: Option<Vec<u8>>,
}

impl ManifestWriterContext {
    /// Create a new ManifestWriterContext
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata_location: String,
        meta_root_path: String,
        commit_uuid: Uuid,
        manifest_counter: Arc<AtomicU64>,
        format_version: FormatVersion,
        snapshot_id: i64,
        file_io: FileIO,
        key_metadata: Option<Vec<u8>>,
    ) -> Self {
        Self {
            metadata_location,
            meta_root_path,
            commit_uuid,
            manifest_counter,
            format_version,
            snapshot_id,
            file_io,
            key_metadata,
        }
    }

    /// Create a manifest writer for the specified content type
    pub fn new_manifest_writer(
        &self,
        content_type: ManifestContentType,
        table_schema: &Schema,
        partition_spec: &PartitionSpec,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = new_manifest_path(
            &self.metadata_location,
            &self.meta_root_path,
            self.commit_uuid,
            self.manifest_counter.fetch_add(1, Ordering::SeqCst),
            crate::spec::DataFileFormat::Avro,
        );

        let output = self.file_io.new_output(&new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            table_schema.clone().into(),
            partition_spec.clone(),
        );

        match self.format_version {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content_type {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content_type {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }
}

/// Manager for filtering manifest files and handling delete operations
pub struct ManifestFilterManager {
    /// Files to be deleted by path
    files_to_delete: HashMap<String, DataFile>,

    /// Minimum sequence number for removing old delete files
    min_sequence_number: i64,
    /// Whether to fail if any delete operation is attempted
    fail_any_delete: bool,
    /// Whether to fail if required delete paths are missing
    fail_missing_delete_paths: bool,
    /// Cache of filtered manifests to avoid reprocessing
    filtered_manifests: HashMap<String, ManifestFile>, // manifest_path -> filtered_manifest
    /// Tracking where files were deleted to validate retries quickly
    filtered_manifest_to_deleted_files: HashMap<String, Vec<String>>, // manifest_path -> deleted_files
    ///    this is only being used for the DeleteManifestFilterManager to detect orphaned deletes for removed data file paths
    removed_data_file_path: HashSet<String>,

    file_io: FileIO,
    writer_context: ManifestWriterContext,
}

impl ManifestFilterManager {
    /// Create a new ManifestFilterManager
    pub fn new(file_io: FileIO, writer_context: ManifestWriterContext) -> Self {
        Self {
            files_to_delete: HashMap::new(),
            min_sequence_number: 0,
            fail_any_delete: false,
            fail_missing_delete_paths: false,
            filtered_manifests: HashMap::new(),
            filtered_manifest_to_deleted_files: HashMap::new(),
            removed_data_file_path: HashSet::new(),
            file_io,
            writer_context,
        }
    }

    /// Set whether to fail if any delete operation is attempted
    pub fn fail_any_delete(mut self) -> Self {
        self.fail_any_delete = true;
        self
    }

    /// Get files marked for deletion
    pub fn files_to_be_deleted(&self) -> Vec<&DataFile> {
        self.files_to_delete.values().collect()
    }

    /// Set minimum sequence number for removing old delete files
    pub(crate) fn drop_delete_files_older_than(&mut self, sequence_number: i64) {
        assert!(
            sequence_number >= 0,
            "Invalid minimum data sequence number: {sequence_number}",
        );
        self.min_sequence_number = sequence_number;
    }

    /// Set whether to fail if required delete paths are missing
    pub fn fail_missing_delete_paths(mut self) -> Self {
        self.fail_missing_delete_paths = true;
        self
    }

    /// Mark a data file for deletion
    pub fn delete_file(&mut self, file: DataFile) -> Result<()> {
        // Todo: check all deletes references in manifests?
        let file_path = file.file_path.clone();

        self.files_to_delete.insert(file_path, file);

        Ok(())
    }

    /// Check if this manager contains any delete operations
    pub fn contains_deletes(&self) -> bool {
        !self.files_to_delete.is_empty()
    }

    /// Filter manifest files, removing entries marked for deletion
    pub async fn filter_manifests(
        &mut self,
        table_schema: &Schema,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if manifests.is_empty() {
            self.validate_required_deletes(&[])?;
            return Ok(vec![]);
        }

        let mut filtered = Vec::with_capacity(manifests.len());

        for manifest in manifests {
            let filtered_manifest = self.filter_manifest(table_schema, manifest).await?;
            filtered.push(filtered_manifest);
        }

        self.validate_required_deletes(&filtered)?;
        Ok(filtered)
    }

    /// Filter a single manifest file
    async fn filter_manifest(
        &mut self,
        table_schema: &Schema,
        manifest: ManifestFile,
    ) -> Result<ManifestFile> {
        // Check cache first
        if let Some(cached) = self.filtered_manifests.get(&manifest.manifest_path) {
            return Ok(cached.clone());
        }

        // Check if this manifest can contain files to delete
        if !self.can_contain_deleted_files(&manifest) {
            self.filtered_manifests
                .insert(manifest.manifest_path.clone(), manifest.clone());
            return Ok(manifest);
        }

        if self.manifest_has_deleted_files(&manifest).await? {
            // Load and filter the manifest
            self.filter_manifest_with_deleted_files(table_schema, manifest)
                .await
        } else {
            // If no deleted files are found, just return the original manifest
            self.filtered_manifests
                .insert(manifest.manifest_path.clone(), manifest.clone());
            Ok(manifest)
        }
    }

    /// Check if a manifest can potentially contain files that need to be deleted
    fn can_contain_deleted_files(&self, manifest: &ManifestFile) -> bool {
        // If manifest has no live files, it can't contain files to delete
        if Self::manifest_has_no_live_files(manifest) {
            return false;
        }

        // If we have file-based deletes, any manifest with live files might contain them
        !self.files_to_delete.is_empty() || !self.removed_data_file_path.is_empty()
    }

    /// Filter a manifest that is known to contain files to delete
    async fn filter_manifest_with_deleted_files(
        &mut self,
        table_schema: &Schema,
        manifest: ManifestFile,
    ) -> Result<ManifestFile> {
        // Load the original manifest
        let original_manifest = manifest.load_manifest(&self.file_io).await?;

        let (entries, manifest_meta_data) = original_manifest.into_parts();

        // Check if this is a delete manifest
        let is_delete = manifest.content == ManifestContentType::Deletes;

        // Track deleted files for duplicate detection
        let mut deleted_files = HashMap::new();

        // Create an output path for the filtered manifest using writer context
        let partition_spec = manifest_meta_data.partition_spec.clone();

        // Create the manifest writer using the writer context
        let mut writer = self.writer_context.new_manifest_writer(
            manifest.content,
            table_schema,
            &partition_spec,
        )?;

        // Process each live entry in the manifest
        for entry in &entries {
            if !entry.is_alive() {
                continue;
            }

            let entry = entry.as_ref();
            let file = entry.data_file();

            // Check if file is marked for deletion based on various criteria
            let marked_for_delete =
                // Check if file is in delete files collection
                self.files_to_delete.contains_key(file.file_path()) ||
                // For delete manifests, check sequence number for old delete files
                (is_delete && matches!(entry.sequence_number(), Some(seq_num) if seq_num != crate::spec::UNASSIGNED_SEQUENCE_NUMBER
                             && seq_num > 0
                             && seq_num < self.min_sequence_number));

            // TODO: Add expression evaluation logic
            if marked_for_delete {
                // Check if all rows match
                let all_rows_match = marked_for_delete;

                // Validation check: cannot delete file where some, but not all, rows match filter
                // unless it's a delete file (ignore delete files where some records may not match)
                if !all_rows_match && !is_delete {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete file where some, but not all, rows match filter: {}",
                            file.file_path()
                        ),
                    ));
                }

                if all_rows_match {
                    // Mark this entry as deleted
                    writer.add_delete_entry(entry.clone())?;

                    // Create a copy of the file without stats
                    let file_copy = file.clone();

                    // For file that it was deleted using an expression
                    self.files_to_delete
                        .insert(file.file_path().to_string(), file_copy.clone());

                    // TODO: add file to removed_data_file_path once we implement drop_partition

                    // Track deleted files for duplicate detection and validation
                    if deleted_files.contains_key(file_copy.file_path()) {
                        // TODO: Log warning about duplicate
                    } else {
                        // Only add the file to deletes if it is a new delete
                        // This keeps the snapshot summary accurate for non-duplicate data
                        deleted_files.insert(file_copy.file_path.to_owned(), file_copy.clone());
                    }
                } else {
                    // Keep the entry as existing
                    writer.add_existing_entry(entry.clone())?;
                }
            } else {
                // Keep the entry as existing
                writer.add_existing_entry(entry.clone())?;
            }
        }

        // Write the filtered manifest
        let filtered_manifest = writer.write_manifest_file().await?;

        // Update caches
        self.filtered_manifests
            .insert(manifest.manifest_path.clone(), filtered_manifest.clone());

        // Track deleted files for validation - convert HashSet to Vec of file paths
        let deleted_file_paths: Vec<String> = deleted_files.keys().cloned().collect();

        self.filtered_manifest_to_deleted_files
            .insert(filtered_manifest.manifest_path.clone(), deleted_file_paths);

        Ok(filtered_manifest)
    }

    /// Validate that all required delete operations were found
    fn validate_required_deletes(&self, manifests: &[ManifestFile]) -> Result<()> {
        if self.fail_missing_delete_paths {
            let deleted_files = self.deleted_files(manifests);
            // check deleted_files contains all files in self.delete_files

            for file_path in self.files_to_delete.keys() {
                if !deleted_files.contains(file_path) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Required delete path missing: {file_path}"),
                    ));
                }
            }
        }
        Ok(())
    }

    fn deleted_files(&self, manifests: &[ManifestFile]) -> HashSet<String> {
        let mut deleted_files = HashSet::new();
        for manifest in manifests {
            if let Some(deleted) = self
                .filtered_manifest_to_deleted_files
                .get(manifest.manifest_path.as_str())
            {
                deleted_files.extend(deleted.clone());
            }
        }
        deleted_files
    }

    fn manifest_has_no_live_files(manifest: &ManifestFile) -> bool {
        !manifest.has_added_files() && !manifest.has_existing_files()
    }

    async fn manifest_has_deleted_files(&self, manifest_file: &ManifestFile) -> Result<bool> {
        let manifest = manifest_file.load_manifest(&self.file_io).await?;

        let is_delete = manifest_file.content == ManifestContentType::Deletes;

        for entry in manifest.entries() {
            let entry = entry.as_ref();

            // Skip entries that are already deleted
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }

            let file = entry.data_file();

            // Check if file is marked for deletion based on various criteria
            let marked_for_delete =
                // Check if file path is in files to delete
                self.files_to_delete.contains_key(file.file_path()) ||
                // For delete manifests, check sequence number for old delete files
                (is_delete &&
                 entry.status() != ManifestStatus::Deleted &&
                  matches!(entry.sequence_number(), Some(seq_num) if seq_num != crate::spec::UNASSIGNED_SEQUENCE_NUMBER
                             && seq_num > 0
                             && seq_num < self.min_sequence_number));

            // TODO: Add dangling delete vector check: (is_delete && self.is_dangling_dv(file))

            // TODO: Add expression evaluation logic
            if marked_for_delete {
                // Check if all rows match
                let all_rows_match = marked_for_delete; // || evaluator.rowsMustMatch(file) equivalent

                // Validation check: cannot delete file where some, but not all, rows match filter
                // unless it's a delete file
                if !all_rows_match && !is_delete {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot delete file where some, but not all, rows match filter: {}",
                            file.file_path()
                        ),
                    ));
                }

                if all_rows_match {
                    // Check fail_any_delete flag
                    if self.fail_any_delete {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Operation would delete existing data".to_string(),
                        ));
                    }

                    // As soon as a deleted file is detected, stop scanning and return true
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub(crate) fn remove_dangling_deletes_for(&mut self, file_paths: &HashSet<String>) {
        self.removed_data_file_path
            .extend(file_paths.iter().cloned());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
        ManifestFile, ManifestStatus, ManifestWriterBuilder, NestedField, PartitionSpec,
        PrimitiveType, Schema, Struct, Type,
    };

    // Helper function to create a test schema
    fn create_test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    // Helper function to create a test DataFile
    fn create_test_data_file(file_path: &str, partition_spec_id: i32) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: file_path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            partition_spec_id,
            record_count: 100,
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
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    // Helper function to create a test ManifestFile with default values
    fn create_test_manifest_file(
        manifest_path: &str,
        content: ManifestContentType,
    ) -> ManifestFile {
        create_manifest_with_counts(manifest_path, content, 10, 5, 0)
    }

    // Helper function to create a ManifestFile with custom counts
    fn create_manifest_with_counts(
        manifest_path: &str,
        content: ManifestContentType,
        added_files: u32,
        existing_files: u32,
        deleted_files: u32,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: manifest_path.to_string(),
            manifest_length: 5000,
            partition_spec_id: 0,
            content,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 12345,
            added_files_count: Some(added_files),
            existing_files_count: Some(existing_files),
            deleted_files_count: Some(deleted_files),
            added_rows_count: Some(added_files as u64 * 100),
            existing_rows_count: Some(existing_files as u64 * 100),
            deleted_rows_count: Some(deleted_files as u64 * 100),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    // Helper function to create a ManifestFile with specific sequence numbers
    fn create_manifest_with_sequence(
        manifest_path: &str,
        content: ManifestContentType,
        sequence_number: i64,
        min_sequence_number: i64,
    ) -> ManifestFile {
        let mut manifest = create_test_manifest_file(manifest_path, content);
        manifest.sequence_number = sequence_number;
        manifest.min_sequence_number = min_sequence_number;
        manifest
    }

    // Helper function to setup test environment
    fn setup_test_manager() -> (ManifestFilterManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let metadata_location = temp_dir
            .path()
            .join("metadata.json")
            .to_string_lossy()
            .to_string();
        let meta_root_path = temp_dir.path().to_string_lossy().to_string();

        let writer_context = ManifestWriterContext::new(
            metadata_location,
            meta_root_path,
            Uuid::new_v4(),
            Arc::new(AtomicU64::new(0)),
            FormatVersion::V2,
            1,
            file_io.clone(),
            None,
        );

        let manager = ManifestFilterManager::new(file_io, writer_context);

        (manager, temp_dir)
    }

    // Helper function to write manifest entries to file
    async fn write_manifest_with_entries(
        manager: &ManifestFilterManager,
        manifest_path: &str,
        schema: &Schema,
        entries: Vec<ManifestEntry>,
        snapshot_id: i64,
    ) -> Result<()> {
        let partition_spec = PartitionSpec::unpartition_spec();
        let output_file = manager.file_io.new_output(manifest_path)?;
        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(snapshot_id),
            None,
            schema.clone().into(),
            partition_spec,
        )
        .build_v2_data();

        for entry in entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await?;
        Ok(())
    }

    // Helper function to create manifest entries from data files
    fn create_entries_from_files(files: Vec<DataFile>) -> Vec<ManifestEntry> {
        files
            .into_iter()
            .map(|file| {
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(file)
                    .build()
            })
            .collect()
    }

    // Helper function to create a full manifest file metadata
    fn create_manifest_metadata(
        manifest_path: &str,
        content: ManifestContentType,
        sequence_number: i64,
        snapshot_id: i64,
        file_counts: (u32, u32, u32), // (added, existing, deleted)
    ) -> ManifestFile {
        let (added, existing, deleted) = file_counts;
        ManifestFile {
            manifest_path: manifest_path.to_string(),
            manifest_length: 1000,
            partition_spec_id: 0,
            content,
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(added),
            existing_files_count: Some(existing),
            deleted_files_count: Some(deleted),
            added_rows_count: Some(added as u64 * 100),
            existing_rows_count: Some(existing as u64 * 100),
            deleted_rows_count: Some(deleted as u64 * 100),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn test_new_manifest_filter_manager() {
        let (manager, _temp_dir) = setup_test_manager();

        // Test initial state
        assert!(!manager.contains_deletes());
        assert_eq!(manager.min_sequence_number, 0);
        assert!(!manager.fail_any_delete);
        assert!(!manager.fail_missing_delete_paths);
        assert!(manager.files_to_delete.is_empty());
    }

    #[test]
    fn test_configuration_flags() {
        let (manager, _temp_dir) = setup_test_manager();

        let mut configured_manager = manager.fail_any_delete().fail_missing_delete_paths();
        configured_manager.drop_delete_files_older_than(100);

        assert!(configured_manager.fail_any_delete);
        assert!(configured_manager.fail_missing_delete_paths);
        assert_eq!(configured_manager.min_sequence_number, 100);
    }

    #[test]
    fn test_delete_file() {
        let (mut manager, _temp_dir) = setup_test_manager();

        // Test 1: Delete file by path
        let file_path = "/test/path/file.parquet";
        let test_file1 = create_test_data_file(file_path, 0);

        // Initially no deletes
        assert!(!manager.contains_deletes());

        // Add file to delete
        manager.delete_file(test_file1).unwrap();

        // Should now contain deletes
        assert!(manager.contains_deletes());
        assert!(manager.files_to_delete.contains_key(file_path));

        // Test 2: Delete another file and verify tracking
        let test_file2 = create_test_data_file("/test/data/file2.parquet", 0);
        let file_path2 = test_file2.file_path.clone();
        manager.delete_file(test_file2).unwrap();

        // Should track both files for deletion
        let deleted_files = manager.files_to_be_deleted();
        assert_eq!(deleted_files.len(), 2);
        assert!(manager.files_to_delete.contains_key(&file_path2));
    }

    #[test]
    fn test_manifest_has_no_live_files() {
        // Test manifest with no live files
        let manifest_no_live =
            create_manifest_with_counts("/test/manifest1.avro", ManifestContentType::Data, 0, 0, 5);
        assert!(ManifestFilterManager::manifest_has_no_live_files(
            &manifest_no_live
        ));

        // Test manifest with live files
        let manifest_with_live =
            create_test_manifest_file("/test/manifest2.avro", ManifestContentType::Data);
        assert!(!ManifestFilterManager::manifest_has_no_live_files(
            &manifest_with_live
        ));
    }

    #[test]
    fn test_can_contain_deleted_files() {
        let (mut manager, _temp_dir) = setup_test_manager();

        // Manifest with no live files should not contain deleted files
        let manifest_no_live =
            create_manifest_with_counts("/test/manifest1.avro", ManifestContentType::Data, 0, 0, 5);
        assert!(!manager.can_contain_deleted_files(&manifest_no_live));

        // Manifest with live files but no deletes
        let manifest_with_live =
            create_test_manifest_file("/test/manifest2.avro", ManifestContentType::Data);
        assert!(!manager.can_contain_deleted_files(&manifest_with_live));

        // Add deletes and test again
        let test_file = create_test_data_file("/test/file.parquet", 0);
        manager.delete_file(test_file).unwrap();
        assert!(manager.can_contain_deleted_files(&manifest_with_live));
    }

    #[tokio::test]
    async fn test_filter_manifests_empty_input() {
        let (mut manager, _temp_dir) = setup_test_manager();
        let schema = create_test_schema();

        let result = manager.filter_manifests(&schema, vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_validate_required_deletes_success() {
        let (manager, _temp_dir) = setup_test_manager();

        // Test validation with no required deletes
        let manifests = vec![create_test_manifest_file(
            "/test/manifest.avro",
            ManifestContentType::Data,
        )];
        let result = manager.validate_required_deletes(&manifests);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_required_deletes_failure() {
        let (mut manager, _temp_dir) = setup_test_manager();

        // Enable fail_missing_delete_paths
        manager.fail_missing_delete_paths = true;

        // Add a required delete file that won't be found
        let missing_file = create_test_data_file("/missing/file.parquet", 0);
        manager.delete_file(missing_file).unwrap();

        let manifests = vec![create_test_manifest_file(
            "/test/manifest.avro",
            ManifestContentType::Data,
        )];
        let result = manager.validate_required_deletes(&manifests);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Required delete path missing")
        );
    }

    #[tokio::test]
    async fn test_comprehensive_deletion_logic() {
        let (mut manager, temp_dir) = setup_test_manager();
        let schema = create_test_schema();

        // Create test data files
        let keep_file = create_test_data_file("/test/keep_me.parquet", 0);
        let delete_file = create_test_data_file("/test/delete_me.parquet", 0);
        manager.delete_file(delete_file.clone()).unwrap();

        // Write manifest with both files
        let manifest_path = temp_dir.path().join("test_manifest.avro");
        let manifest_path_str = manifest_path.to_str().unwrap();
        let entries = create_entries_from_files(vec![keep_file.clone(), delete_file.clone()]);
        write_manifest_with_entries(&manager, manifest_path_str, &schema, entries, 12345)
            .await
            .unwrap();

        // Create ManifestFile metadata
        let manifest = create_manifest_metadata(
            manifest_path_str,
            ManifestContentType::Data,
            10,
            12345,
            (2, 0, 0),
        );

        // Verify manifest filtering capabilities
        assert!(manager.can_contain_deleted_files(&manifest));
        assert!(manager.manifest_has_deleted_files(&manifest).await.unwrap());
        assert!(manager.files_to_delete.contains_key(&delete_file.file_path));
        assert!(!manager.files_to_delete.contains_key(&keep_file.file_path));

        // Verify manager state
        assert!(manager.contains_deletes());
        let files_to_delete = manager.files_to_be_deleted();
        assert_eq!(files_to_delete.len(), 1);
        assert_eq!(files_to_delete[0].file_path, delete_file.file_path);
    }

    #[test]
    fn test_min_sequence_number_logic() {
        let (mut manager, _temp_dir) = setup_test_manager();
        manager.min_sequence_number = 5;

        // Test basic sequence number comparison logic
        assert_eq!(manager.min_sequence_number, 5);
        let (old_sequence, new_sequence) = (3, 10);
        assert!(old_sequence < manager.min_sequence_number);
        assert!(new_sequence >= manager.min_sequence_number);

        // Create manifests with different sequence numbers
        let old_manifest = create_manifest_with_sequence(
            "/test/old.avro",
            ManifestContentType::Data,
            old_sequence,
            old_sequence,
        );
        let new_manifest = create_manifest_with_sequence(
            "/test/new.avro",
            ManifestContentType::Data,
            new_sequence,
            new_sequence,
        );

        // Add files to delete for testing
        manager
            .delete_file(create_test_data_file("/test/file.parquet", 0))
            .unwrap();

        // Both manifests should be able to contain deleted files (key filtering behavior)
        assert!(manager.can_contain_deleted_files(&old_manifest));
        assert!(manager.can_contain_deleted_files(&new_manifest));

        // Verify sequence number properties
        assert!(old_manifest.min_sequence_number < manager.min_sequence_number);
        assert!(new_manifest.min_sequence_number >= manager.min_sequence_number);
    }

    #[test]
    fn test_deletion_tracking_and_validation() {
        let (mut manager, _temp_dir) = setup_test_manager();

        let delete_file = create_test_data_file("/test/delete_me.parquet", 0);

        // Initially no deletes
        assert!(!manager.contains_deletes());
        assert_eq!(manager.files_to_be_deleted().len(), 0);

        // Add the file to be deleted
        manager.delete_file(delete_file.clone()).unwrap();

        // Verify deletion tracking
        assert!(manager.contains_deletes());
        assert_eq!(manager.files_to_be_deleted().len(), 1);
        assert_eq!(
            manager.files_to_be_deleted()[0].file_path,
            delete_file.file_path
        );

        // Create a manifest that could contain deleted files
        let manifest = create_manifest_with_sequence(
            "/test/test_manifest.avro",
            ManifestContentType::Data,
            10,
            1,
        );

        // Verify manifest can contain deleted files
        assert!(manager.can_contain_deleted_files(&manifest));
        assert!(manager.files_to_delete.contains_key(&delete_file.file_path));

        // Validation should pass when no required deletes are set
        assert!(manager.validate_required_deletes(&[manifest]).is_ok());
    }

    #[tokio::test]
    async fn test_filter_manifests_with_entries_and_rewrite() {
        let (mut manager, temp_dir) = setup_test_manager();
        let schema = create_test_schema();

        // Create test files
        let files_to_keep = [
            create_test_data_file("/test/keep1.parquet", 0),
            create_test_data_file("/test/keep2.parquet", 0),
        ];
        let files_to_delete = vec![
            create_test_data_file("/test/delete1.parquet", 0),
            create_test_data_file("/test/delete2.parquet", 0),
        ];

        // Mark files for deletion
        for file in &files_to_delete {
            manager.delete_file(file.clone()).unwrap();
        }

        // Create two manifests with mixed files
        let manifest_paths: Vec<_> = (1..=2)
            .map(|i| {
                temp_dir
                    .path()
                    .join(format!("manifest{i}.avro"))
                    .to_string_lossy()
                    .to_string()
            })
            .collect();

        for (idx, path) in manifest_paths.iter().enumerate() {
            let entries = create_entries_from_files(vec![
                files_to_keep[idx].clone(),
                files_to_delete[idx].clone(),
            ]);
            write_manifest_with_entries(&manager, path, &schema, entries, 12345 + idx as i64)
                .await
                .unwrap();
        }

        // Create ManifestFile metadata objects
        let input_manifests: Vec<_> = manifest_paths
            .iter()
            .enumerate()
            .map(|(idx, path)| {
                create_manifest_metadata(
                    path,
                    ManifestContentType::Data,
                    10,
                    12345 + idx as i64,
                    (2, 0, 0),
                )
            })
            .collect();

        // Filter manifests
        let filtered_manifests = manager
            .filter_manifests(&schema, input_manifests.clone())
            .await
            .unwrap();

        // Verify results
        assert_eq!(filtered_manifests.len(), 2);
        assert_ne!(
            filtered_manifests[0].manifest_path,
            input_manifests[0].manifest_path
        );
        assert_ne!(
            filtered_manifests[1].manifest_path,
            input_manifests[1].manifest_path
        );

        // Verify deletion tracking
        assert_eq!(manager.files_to_be_deleted().len(), 2);
        let deleted_paths: std::collections::HashSet<_> = manager
            .files_to_be_deleted()
            .into_iter()
            .map(|f| f.file_path.clone())
            .collect();
        for file in &files_to_delete {
            assert!(deleted_paths.contains(&file.file_path));
        }

        // Verify filtered manifest entries
        let filtered_manifest1 = filtered_manifests[0]
            .load_manifest(&manager.file_io)
            .await
            .unwrap();
        let (entries_filtered, _) = filtered_manifest1.into_parts();

        let (live_count, deleted_count) =
            entries_filtered
                .iter()
                .fold((0, 0), |(live, deleted), entry| match entry.status() {
                    ManifestStatus::Added | ManifestStatus::Existing => (live + 1, deleted),
                    ManifestStatus::Deleted => (live, deleted + 1),
                });

        assert_eq!(live_count, 1);
        assert_eq!(deleted_count, 1);

        // Verify cache and tracking
        assert!(
            manager
                .filtered_manifests
                .contains_key(&input_manifests[0].manifest_path)
        );
        assert!(
            manager
                .filtered_manifest_to_deleted_files
                .contains_key(&filtered_manifests[0].manifest_path)
        );
    }

    #[test]
    fn test_unassigned_sequence_number_handling() {
        let (mut manager, _temp_dir) = setup_test_manager();
        manager.drop_delete_files_older_than(100);

        // Create manifest with UNASSIGNED_SEQUENCE_NUMBER and no live files
        let manifest_unassigned = create_manifest_with_sequence(
            "/test/unassigned.avro",
            ManifestContentType::Deletes,
            crate::spec::UNASSIGNED_SEQUENCE_NUMBER,
            crate::spec::UNASSIGNED_SEQUENCE_NUMBER,
        );

        // Manifest with no live files should not contain deleted files
        assert!(!manager.can_contain_deleted_files(&manifest_unassigned));
    }

    #[tokio::test]
    async fn test_cache_behavior() {
        let (mut manager, temp_dir) = setup_test_manager();
        let schema = create_test_schema();

        // Create and write a manifest without deleted files
        let manifest_path = temp_dir.path().join("cache_test.avro");
        let manifest_path_str = manifest_path.to_str().unwrap();

        let entries =
            create_entries_from_files(vec![create_test_data_file("/test/keep.parquet", 0)]);
        write_manifest_with_entries(&manager, manifest_path_str, &schema, entries, 12345)
            .await
            .unwrap();

        let manifest = create_manifest_metadata(
            manifest_path_str,
            ManifestContentType::Data,
            10,
            12345,
            (1, 0, 0),
        );

        // First and second calls should return the same cached result
        let result1 = manager
            .filter_manifest(&schema, manifest.clone())
            .await
            .unwrap();
        let result2 = manager
            .filter_manifest(&schema, manifest.clone())
            .await
            .unwrap();

        assert_eq!(result1.manifest_path, result2.manifest_path);
        assert!(
            manager
                .filtered_manifests
                .contains_key(&manifest.manifest_path)
        );
    }

    #[test]
    fn test_batch_delete_operations() {
        let (mut manager, _temp_dir) = setup_test_manager();

        assert!(!manager.contains_deletes());

        // Add multiple files for deletion
        for i in 1..=3 {
            manager
                .delete_file(create_test_data_file(&format!("/test/batch{i}.parquet"), 0))
                .unwrap();
        }

        // Verify all files are tracked
        assert!(manager.contains_deletes());
        assert_eq!(manager.files_to_be_deleted().len(), 3);

        let deleted_paths: std::collections::HashSet<_> = manager
            .files_to_be_deleted()
            .iter()
            .map(|f| f.file_path.as_str())
            .collect();
        assert!(deleted_paths.contains("/test/batch1.parquet"));
        assert!(deleted_paths.contains("/test/batch2.parquet"));
        assert!(deleted_paths.contains("/test/batch3.parquet"));
    }

    #[test]
    fn test_edge_case_empty_partition_specs() {
        let (mut manager, _temp_dir) = setup_test_manager();

        // Create a data file with different partition spec
        let file_with_different_spec = DataFile {
            content: crate::spec::DataContentType::Data,
            file_path: "/test/different_spec.parquet".to_string(),
            file_format: crate::spec::DataFileFormat::Parquet,
            partition: crate::spec::Struct::empty(),
            partition_spec_id: 999, // Different spec ID
            record_count: 100,
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
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };

        // Should be able to add file with different partition spec
        manager.delete_file(file_with_different_spec).unwrap();
        assert!(manager.contains_deletes());
        assert_eq!(manager.files_to_be_deleted().len(), 1);
    }
}
