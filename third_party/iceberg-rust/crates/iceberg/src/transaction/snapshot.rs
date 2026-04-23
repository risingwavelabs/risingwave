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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, Operation, PrimitiveLiteral, Snapshot, SnapshotReference,
    SnapshotRetention, SnapshotSummaryCollector, Struct, StructType, Summary, TableProperties,
    UNASSIGNED_SEQUENCE_NUMBER, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, ManifestFilterManager, ManifestWriterContext};
use crate::utils::bin::ListPacker;
use crate::utils::load_manifests;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// A trait that defines how different table operations produce new snapshots.
///
/// `SnapshotProduceOperation` is used by [`SnapshotProducer`] to customize snapshot creation
/// based on the type of operation being performed (e.g., `Append`, `Overwrite`, `Delete`, etc.).
/// Each operation type implements this trait to specify:
/// - Which operation type to record in the snapshot summary
/// - Which existing manifest files should be included in the new snapshot
/// - Which manifest entries should be marked as deleted
///
/// # When it accomplishes
///
/// This trait is used during the snapshot creation process in [`SnapshotProducer::commit()`]:
///
/// 1. **Operation Type Recording**: The `operation()` method determines which operation type
///    (e.g., `Operation::Append`, `Operation::Overwrite`) is recorded in the snapshot summary.
///    This metadata helps track what kind of change was made to the table.
///
/// 2. **Manifest File Selection**: The `existing_manifest()` method determines which existing
///    manifest files from the current snapshot should be carried forward to the new snapshot.
///    For example:
///    - An `Append` operation typically includes all existing manifests plus new ones
///    - An `Overwrite` operation might exclude manifests for partitions being overwritten
///
/// 3. **Delete Entry Processing**: The `delete_entries()` method is intended for future delete
///    operations to specify which manifest entries should be marked as deleted.
pub(crate) trait SnapshotProduceOperation: Send + Sync {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;
    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    #[allow(unused)]
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct DefaultManifestProcess;

#[async_trait]
impl ManifestProcess for DefaultManifestProcess {
    async fn process_manifests(
        &self,
        _snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        Ok(manifests)
    }
}

#[async_trait]
pub(crate) trait ManifestProcess: Send + Sync {
    async fn process_manifests(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>>;
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    pub added_data_files: Vec<DataFile>,
    pub added_delete_files: Vec<DataFile>,

    // for filtering out files that are removed by action
    pub removed_data_file_paths: HashSet<String>,
    pub removed_delete_file_paths: HashSet<String>,
    pub removed_delete_files: Vec<DataFile>,

    // A counter used to generate unique manifest file names.
    // It starts from 0 and increments for each new manifest file.
    // This counter is shared with ManifestWriterContext to avoid naming conflicts.
    manifest_counter: Arc<AtomicU64>,

    new_data_file_sequence_number: Option<i64>,

    target_branch: String,

    delete_filter_manager: Option<ManifestFilterManager>,
}

impl<'a> SnapshotProducer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_id: Option<i64>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
        added_delete_files: Vec<DataFile>,
        removed_data_files: Vec<DataFile>,
        removed_delete_files: Vec<DataFile>,
    ) -> Self {
        let removed_data_file_paths = removed_data_files
            .into_iter()
            .map(|df| df.file_path)
            .collect();
        let removed_delete_file_paths = removed_delete_files
            .iter()
            .map(|df| df.file_path.clone())
            .collect();

        let manifest_counter = Arc::new(AtomicU64::new(0));

        // Default: disable delete filter manager (need to explicitly enable)
        let delete_filter_manager = None;

        Self {
            table,
            snapshot_id: snapshot_id.unwrap_or_else(|| Self::generate_unique_snapshot_id(table)),
            commit_uuid,
            snapshot_properties,
            manifest_counter,
            key_metadata,
            added_data_files,
            added_delete_files,
            removed_data_file_paths,
            removed_delete_file_paths,
            removed_delete_files,
            new_data_file_sequence_number: None,
            target_branch: MAIN_BRANCH.to_string(),
            delete_filter_manager,
        }
    }

    pub(crate) fn validate_added_data_files(&self, added_data_files: &[DataFile]) -> Result<()> {
        for data_file in added_data_files {
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            Self::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    // pub(crate) async fn validate_duplicate_files(
    //     &self,
    //     added_data_files: &[DataFile],
    // ) -> Result<()> {
    //     let new_files: HashSet<&str> = added_data_files
    //         .iter()
    //         .map(|df| df.file_path.as_str())
    //         .collect();

    //     let mut referenced_files = Vec::new();
    //     if let Some(current_snapshot) = self.table.metadata().snapshot_for_ref(&self.target_branch)
    //     {
    //         let manifest_list = current_snapshot
    //             .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
    //             .await?;
    //         for manifest_list_entry in manifest_list.entries() {
    //             let manifest = manifest_list_entry
    //                 .load_manifest(self.table.file_io())
    //                 .await?;
    //             for entry in manifest.entries() {
    //                 let file_path = entry.file_path();
    //                 if new_files.contains(file_path) && entry.is_alive() {
    //                     referenced_files.push(file_path.to_string());
    //                 }
    //             }
    //         }
    //     }

    //     if !referenced_files.is_empty() {
    //         return Err(Error::new(
    //             ErrorKind::DataInvalid,
    //             format!(
    //                 "Cannot add files that are already referenced by table, files: {}",
    //                 referenced_files.join(", ")
    //             ),
    //         ));
    //     }

    //     Ok(())
    // }

    pub(crate) fn generate_unique_snapshot_id(table: &Table) -> i64 {
        let generate_random_id = || -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let snapshot_id = (lhs ^ rhs) as i64;
            if snapshot_id < 0 {
                -snapshot_id
            } else {
                snapshot_id
            }
        };
        let mut snapshot_id = generate_random_id();

        while table
            .metadata()
            .snapshots()
            .any(|s| s.snapshot_id() == snapshot_id)
        {
            snapshot_id = generate_random_id();
        }
        snapshot_id
    }

    pub(crate) fn new_manifest_writer(
        &mut self,
        content: ManifestContentType,
        partition_spec_id: i32,
    ) -> Result<ManifestWriter> {
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            self.manifest_counter.fetch_add(1, Ordering::SeqCst),
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            self.table
                .metadata()
                .partition_spec_by_id(partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Invalid partition spec id for new manifest writer",
                    )
                    .with_context("partition spec id", partition_spec_id.to_string())
                })?
                .as_ref()
                .clone(),
        );
        match self.table.metadata().format_version() {
            FormatVersion::V1 => Ok(builder.build_v1()),
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    // Check if the partition value is compatible with the partition type.
    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        fn literal_type_name(literal: &PrimitiveLiteral) -> &'static str {
            match literal {
                PrimitiveLiteral::Boolean(_) => "boolean",
                PrimitiveLiteral::Int(_) => "int",
                PrimitiveLiteral::Long(_) => "long",
                PrimitiveLiteral::Float(_) => "float",
                PrimitiveLiteral::Double(_) => "double",
                PrimitiveLiteral::String(_) => "string",
                PrimitiveLiteral::Binary(_) => "binary",
                PrimitiveLiteral::Int128(_) => "decimal",
                PrimitiveLiteral::UInt128(_) => "uuid",
                PrimitiveLiteral::AboveMax => "above_max",
                PrimitiveLiteral::BelowMin => "below_min",
            }
        }

        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Partition struct field count mismatch. struct_value: {:?} ({} fields), partition_type: {:?} ({} fields)",
                    partition_value,
                    partition_value.fields().len(),
                    partition_type,
                    partition_type.fields().len()
                ),
            ));
        }

        for (idx, (value, field)) in partition_value
            .fields()
            .iter()
            .zip(partition_type.fields())
            .enumerate()
        {
            let field = field.field_type.as_primitive_type().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition field should only be primitive type.",
                )
            })?;
            if let Some(value) = value {
                let literal = value.as_primitive_literal().unwrap();
                if !field.compatible(&literal) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Partition field value {:?} (type {}) is not compatible with partition field type {} at index {}. \
partition_struct: {:?}, partition_type: {:?}",
                            literal,
                            literal_type_name(&literal),
                            field,
                            idx,
                            partition_value,
                            partition_type
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(
        &mut self,
        added_files: Vec<DataFile>,
        data_seq: Option<i64>,
    ) -> Result<ManifestFile> {
        if added_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let file_count = added_files.len();

        let manifest_content_type = {
            let mut data_num = 0;
            let mut delete_num = 0;
            for f in &added_files {
                match f.content_type() {
                    DataContentType::Data => data_num += 1,
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        delete_num += 1
                    }
                }
            }
            if data_num == file_count {
                ManifestContentType::Data
            } else if delete_num == file_count {
                ManifestContentType::Deletes
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "added DataFile for a ManifestFile should be same type (Data or Delete)",
                ));
            }
        };

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let manifest_entries = added_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file)
                .sequence_number_opt(data_seq);

            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });

        let mut writer = self.new_manifest_writer(
            manifest_content_type,
            self.table.metadata().default_partition_spec_id(),
        )?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_delete_manifest(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        if deleted_entries.is_empty() {
            return Ok(vec![]);
        }

        // Group deleted entries by spec_id
        let mut partition_groups = HashMap::new();
        for entry in deleted_entries {
            partition_groups
                .entry(entry.data_file().partition_spec_id)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        // Write a delete manifest per spec_id group
        let mut deleted_manifests = Vec::new();
        for (spec_id, entries) in partition_groups {
            let mut data_file_writer: Option<ManifestWriter> = None;
            let mut delete_file_writer: Option<ManifestWriter> = None;
            for entry in entries {
                match entry.content_type() {
                    DataContentType::Data => {
                        if data_file_writer.is_none() {
                            data_file_writer =
                                Some(self.new_manifest_writer(ManifestContentType::Data, spec_id)?);
                        }
                        data_file_writer.as_mut().unwrap().add_delete_entry(entry)?;
                    }
                    DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                        if delete_file_writer.is_none() {
                            delete_file_writer = Some(
                                self.new_manifest_writer(ManifestContentType::Deletes, spec_id)?,
                            );
                        }
                        delete_file_writer
                            .as_mut()
                            .unwrap()
                            .add_delete_entry(entry)?;
                    }
                }
            }
            if let Some(writer) = data_file_writer {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
            if let Some(writer) = delete_file_writer {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
        }

        Ok(deleted_manifests)
    }

    async fn manifest_file<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        &mut self,
        snapshot_produce_operation: &OP,
        manifest_process: &MP,
    ) -> Result<Vec<ManifestFile>> {
        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.snapshot_properties.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_data_file_paths.is_empty()
            && self.removed_delete_file_paths.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files or added snapshot properties found when write a manifest file",
            ));
        }

        // Get existing manifests and prepare them for the manifest list.
        // Existing manifests must come before new manifests in the final list
        // to ensure correct first_row_id assignment by ManifestListWriter.
        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;

        let mut manifest_files =
            if let Some(delete_filter_manager) = self.delete_filter_manager.as_mut() {
                // When delete filter manager is enabled, filter existing manifests
                let metadata_ref = self.table.metadata_ref();
                let branch_snapshot_ref = metadata_ref.snapshot_for_ref(&self.target_branch);

                let schema_id = if let Some(branch_snapshot_ref) = branch_snapshot_ref {
                    branch_snapshot_ref
                        .schema_id()
                        .unwrap_or(metadata_ref.current_schema_id())
                } else {
                    metadata_ref.current_schema_id()
                };

                let schema = metadata_ref
                    .schema_by_id(schema_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Invalid schema id for existing manifest filtering",
                        )
                        .with_context("schema id", schema_id.to_string())
                    })?
                    .as_ref()
                    .clone();

                let last_seq = metadata_ref.last_sequence_number();

                // Partition manifests by type to avoid cloning
                let (existing_data_manifests, existing_delete_manifests): (Vec<_>, Vec<_>) =
                    existing_manifests
                        .into_iter()
                        .partition(|m| matches!(m.content, ManifestContentType::Data));

                let min_data_seq = existing_data_manifests
                    .iter()
                    .map(|m| m.min_sequence_number)
                    .filter(|seq| *seq != UNASSIGNED_SEQUENCE_NUMBER)
                    .reduce(std::cmp::min)
                    .map(|min_seq| std::cmp::min(min_seq, last_seq))
                    .unwrap_or(last_seq);

                let mut filtered_manifests = existing_data_manifests;

                delete_filter_manager.drop_delete_files_older_than(min_data_seq);
                delete_filter_manager.remove_dangling_deletes_for(&self.removed_data_file_paths);

                let filtered_delete_manifests: Vec<ManifestFile> = delete_filter_manager
                    .filter_manifests(&schema, existing_delete_manifests)
                    .await?;
                filtered_manifests.extend(filtered_delete_manifests);

                filtered_manifests.retain(|m| {
                    m.has_added_files()
                        || m.has_existing_files()
                        || m.added_snapshot_id == self.snapshot_id
                });

                filtered_manifests
            } else {
                // No filtering, use existing manifests as-is
                existing_manifests
            };

        // Now append new manifests created in this snapshot.
        // Order matters: existing manifests first, then new manifests.
        // This ensures ManifestListWriter assigns first_row_id correctly.
        if !self.added_data_files.is_empty() {
            let added_data_files = std::mem::take(&mut self.added_data_files);
            let added_manifest = self
                .write_added_manifest(added_data_files, self.new_data_file_sequence_number)
                .await?;
            manifest_files.push(added_manifest);
        }

        if !self.added_delete_files.is_empty() {
            let added_delete_files = std::mem::take(&mut self.added_delete_files);
            let added_manifest = self
                .write_added_manifest(added_delete_files, self.new_data_file_sequence_number)
                .await?;
            manifest_files.push(added_manifest);
        }

        let delete_manifests = self
            .write_delete_manifest(snapshot_produce_operation.delete_entries(self).await?)
            .await?;
        manifest_files.extend(delete_manifests);

        manifest_process
            .process_manifests(self, manifest_files)
            .await
    }

    // Returns a `Summary` of the current snapshot
    fn summary<OP: SnapshotProduceOperation>(
        &self,
        snapshot_produce_operation: &OP,
    ) -> Result<Summary> {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let table_metadata = self.table.metadata_ref();

        let partition_summary_limit = if let Some(limit) = table_metadata
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
        {
            if let Ok(limit) = limit.parse::<u64>() {
                limit
            } else {
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            }
        } else {
            TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
        };

        summary_collector.set_partition_summary_limit(partition_summary_limit);

        for data_file in &self.added_data_files {
            summary_collector.add_file(
                data_file,
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            );
        }

        let previous_snapshot = table_metadata
            .snapshot_by_id(self.snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id())
            .and_then(|parent_id| table_metadata.snapshot_by_id(parent_id));

        let mut additional_properties = summary_collector.build();
        additional_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation: snapshot_produce_operation.operation(),
            additional_properties,
        };

        update_snapshot_summaries(
            summary,
            previous_snapshot.map(|s| s.summary()),
            snapshot_produce_operation.operation() == Operation::Overwrite,
        )
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    /// Finished building the action and return the [`ActionCommit`] to the transaction.
    pub(crate) async fn commit<OP: SnapshotProduceOperation, MP: ManifestProcess>(
        mut self,
        snapshot_produce_operation: OP,
        process: MP,
    ) -> Result<ActionCommit> {
        let manifest_list_path = self.generate_manifest_list_file_path(0);
        let metadata_ref = self.table.metadata_ref();
        let next_seq_num = metadata_ref.next_sequence_number();
        let first_row_id = metadata_ref.next_row_id();
        let parent_snapshot = metadata_ref.snapshot_for_ref(&self.target_branch);

        let parent_snapshot_id = parent_snapshot
            .map(|s| Some(s.snapshot_id()))
            .unwrap_or(None);

        let mut manifest_list_writer = match metadata_ref.format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                parent_snapshot_id,
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                parent_snapshot_id,
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?,
                self.snapshot_id,
                parent_snapshot_id,
                next_seq_num,
                Some(first_row_id),
            ),
        };

        // Calling self.summary() before self.manifest_file() is important because self.added_data_files
        // will be set to an empty vec after self.manifest_file() returns, resulting in an empty summary
        // being generated.
        let summary = self.summary(&snapshot_produce_operation).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to create snapshot summary.").with_source(err)
        })?;

        let new_manifests = self
            .manifest_file(&snapshot_produce_operation, &process)
            .await?;
        manifest_list_writer.add_manifests(new_manifests.into_iter())?;
        let writer_next_row_id = manifest_list_writer.next_row_id();
        manifest_list_writer.close().await?;

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_timestamp_ms(commit_ts);

        let new_snapshot = if let Some(writer_next_row_id) = writer_next_row_id {
            let assigned_rows = writer_next_row_id - self.table.metadata().next_row_id();
            new_snapshot
                .with_row_range(first_row_id, assigned_rows)
                .build()
        } else {
            new_snapshot.build()
        };

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: self.target_branch.clone(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: self.target_branch.clone(),
                snapshot_id: parent_snapshot_id,
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }

    /// Set the new data file sequence number for this snapshot
    pub fn set_new_data_file_sequence_number(&mut self, new_data_file_sequence_number: i64) {
        self.new_data_file_sequence_number = Some(new_data_file_sequence_number);
    }

    /// Replace snapshot properties, overriding any previously set values.
    pub(crate) fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) {
        self.snapshot_properties = properties;
    }

    /// Set the target branch for this snapshot
    pub fn set_target_branch(&mut self, target_branch: String) {
        self.target_branch = target_branch;
    }

    /// Get the target branch for this snapshot
    pub fn target_branch(&self) -> &str {
        &self.target_branch
    }

    /// Enable delete filter manager for this snapshot (lazy initialization)
    /// This will also populate the manager with files already marked for removal
    pub fn enable_delete_filter_manager(&mut self) {
        if self.delete_filter_manager.is_none() {
            let metadata_ref = self.table.metadata_ref();
            let file_io = self.table.file_io();

            let mut manager = ManifestFilterManager::new(
                file_io.clone(),
                ManifestWriterContext::new(
                    metadata_ref.location().to_string(),
                    META_ROOT_PATH.to_string(),
                    self.commit_uuid,
                    self.manifest_counter.clone(),
                    metadata_ref.format_version(),
                    self.snapshot_id,
                    file_io.clone(),
                    self.key_metadata.clone(),
                ),
            );

            // Populate the manager with files that were already marked for deletion
            // This bridges the gap between Action's delete_files() and SnapshotProducer
            for delete_file in &self.removed_delete_files {
                // Only add delete files (not data files) to the filter manager
                if matches!(
                    delete_file.content_type(),
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes
                ) {
                    let _ = manager.delete_file(delete_file.clone());
                }
            }

            self.delete_filter_manager = Some(manager);
        }
    }

    /// Validate data file operations in a single pass through manifests.
    /// This checks both:
    /// 1. Added files don't already exist in the table (duplicate prevention)
    /// 2. Deleted files actually exist in the table (existence validation)
    pub(crate) async fn validate_data_file_changes(&self) -> Result<()> {
        // Early return if nothing to validate
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_data_file_paths.is_empty()
        {
            return Ok(());
        }

        // Use a mutable set - remove files as we find them
        let mut files_to_delete: HashSet<&str> = self
            .removed_data_file_paths
            .iter()
            .map(|s| s.as_str())
            .collect();

        let table = &self.table;
        let branch_snapshot_ref = table.metadata().snapshot_for_ref(self.target_branch());

        // If trying to delete files but no snapshot exists, that's an error
        if !files_to_delete.is_empty() && branch_snapshot_ref.is_none() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot delete files from a table with no current snapshot, files: {}",
                    files_to_delete
                        .iter()
                        .copied()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }

        let files_to_add: HashSet<&str> = self
            .added_data_files
            .iter()
            .chain(self.added_delete_files.iter())
            .map(|df| df.file_path.as_str())
            .collect();

        let mut duplicate_files = Vec::new();

        // Load all manifests concurrently, then scan entries
        if let Some(current_snapshot) = branch_snapshot_ref {
            let manifest_list = current_snapshot
                .load_manifest_list(table.file_io(), table.metadata_ref().as_ref())
                .await?;

            let manifest_files: Vec<_> = manifest_list.entries().to_vec();
            let loaded_manifests = load_manifests(
                table.file_io(),
                manifest_files,
                crate::utils::DEFAULT_LOAD_CONCURRENCY_LIMIT,
            )
            .await?;

            'outer: for (_, manifest) in &loaded_manifests {
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }

                    let file_path = entry.file_path();

                    // Check for duplicate adds
                    if files_to_add.contains(file_path) {
                        duplicate_files.push(file_path.to_string());
                    }

                    // Remove from files_to_delete as we find them
                    // Remaining files in the set don't exist in the snapshot
                    if !files_to_delete.is_empty() {
                        files_to_delete.remove(file_path);
                    }

                    // Early exit optimization: if both checks are done, stop scanning
                    if duplicate_files.len() == files_to_add.len() && files_to_delete.is_empty() {
                        break 'outer;
                    }
                }
            }
        }

        // Validate no duplicate files are being added
        if !duplicate_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot add files that are already referenced by table, files: {}",
                    duplicate_files.join(", ")
                ),
            ));
        }

        // Any remaining files in files_to_delete don't exist in the snapshot
        if !files_to_delete.is_empty() {
            let non_existent_files: Vec<String> =
                files_to_delete.iter().map(|s| s.to_string()).collect();

            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Cannot delete files that are not in the current snapshot, files: {}",
                    non_existent_files.join(", ")
                ),
            ));
        }

        Ok(())
    }
}

pub(crate) struct MergeManifestProcess {
    target_size_bytes: u32,
    min_count_to_merge: u32,
}

impl MergeManifestProcess {
    pub fn new(target_size_bytes: u32, min_count_to_merge: u32) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
        }
    }
}

#[async_trait]
impl ManifestProcess for MergeManifestProcess {
    async fn process_manifests(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let (unmerge_data_manifest, unmerge_delete_manifest): (Vec<_>, Vec<_>) = manifests
            .into_iter()
            .partition(|manifest| matches!(manifest.content, ManifestContentType::Data));

        let mut data_manifest = {
            let manifest_merge_manager = MergeManifestManager::new(
                self.target_size_bytes,
                self.min_count_to_merge,
                ManifestContentType::Data,
            );
            manifest_merge_manager
                .merge_manifest(snapshot_produce, unmerge_data_manifest)
                .await?
        };

        data_manifest.extend(unmerge_delete_manifest);
        Ok(data_manifest)
    }
}

struct MergeManifestManager {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    content: ManifestContentType,
}

impl MergeManifestManager {
    pub fn new(
        target_size_bytes: u32,
        min_count_to_merge: u32,
        content: ManifestContentType,
    ) -> Self {
        Self {
            target_size_bytes,
            min_count_to_merge,
            content,
        }
    }

    fn group_by_spec(&self, manifests: Vec<ManifestFile>) -> BTreeMap<i32, Vec<ManifestFile>> {
        let mut grouped_manifests = BTreeMap::new();
        for manifest in manifests {
            grouped_manifests
                .entry(manifest.partition_spec_id)
                .or_insert_with(Vec::new)
                .push(manifest);
        }
        grouped_manifests
    }

    async fn merge_bin(
        &self,
        snapshot_id: i64,
        file_io: FileIO,
        manifest_bin: Vec<ManifestFile>,
        mut writer: ManifestWriter,
    ) -> Result<ManifestFile> {
        let loaded = load_manifests(
            &file_io,
            manifest_bin,
            crate::utils::DEFAULT_LOAD_CONCURRENCY_LIMIT,
        )
        .await?;

        for (_, manifest) in loaded {
            for manifest_entry in manifest.entries() {
                if manifest_entry.status() == ManifestStatus::Deleted
                    && manifest_entry
                        .snapshot_id()
                        .is_some_and(|id| id == snapshot_id)
                {
                    //only files deleted by this snapshot should be added to the new manifest
                    writer.add_delete_entry(manifest_entry.as_ref().clone())?;
                } else if manifest_entry.status() == ManifestStatus::Added
                    && manifest_entry
                        .snapshot_id()
                        .is_some_and(|id| id == snapshot_id)
                {
                    //added entries from this snapshot are still added, otherwise they should be existing
                    writer.add_entry(manifest_entry.as_ref().clone())?;
                } else if manifest_entry.status() != ManifestStatus::Deleted {
                    // add all non-deleted files from the old manifest as existing files
                    writer.add_existing_entry(manifest_entry.as_ref().clone())?;
                }
            }
        }

        writer.write_manifest_file().await
    }

    async fn merge_group(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        first_manifest: &ManifestFile,
        group_manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        let packer: ListPacker<ManifestFile> = ListPacker::new(self.target_size_bytes);
        let manifest_bins =
            packer.pack(group_manifests, |manifest| manifest.manifest_length as u32);

        let manifest_merge_futures = manifest_bins
            .into_iter()
            .map(|manifest_bin| {
                if manifest_bin.len() == 1 {
                    Ok(Box::pin(async { Ok(manifest_bin) })
                        as Pin<
                            Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>,
                        >)
                }
                //  if the bin has the first manifest (the new data files or an appended manifest file) then only
                //  merge it if the number of manifests is above the minimum count. this is applied only to bins
                //  with an in-memory manifest so that large manifests don't prevent merging older groups.
                else if manifest_bin
                    .iter()
                    .any(|manifest| manifest == first_manifest)
                    && manifest_bin.len() < self.min_count_to_merge as usize
                {
                    Ok(Box::pin(async { Ok(manifest_bin) })
                        as Pin<
                            Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>,
                        >)
                } else {
                    let writer = snapshot_produce.new_manifest_writer(self.content, snapshot_produce.table.metadata().default_partition_spec_id())?;
                    let snapshot_id = snapshot_produce.snapshot_id;
                    let file_io = snapshot_produce.table.file_io().clone();
                    Ok((Box::pin(async move {
                        Ok(vec![
                            self.merge_bin(
                                snapshot_id,
                                file_io,
                                manifest_bin,
                                writer,
                            )
                            .await?,
                        ])
                    }))
                        as Pin<Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>>)
                }
            })
            .collect::<Result<Vec<Pin<Box<dyn Future<Output = Result<Vec<ManifestFile>>> + Send>>>>>()?;

        let merged_bins: Vec<Vec<ManifestFile>> =
            futures::future::join_all(manifest_merge_futures.into_iter())
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

        Ok(merged_bins.into_iter().flatten().collect())
    }

    pub(crate) async fn merge_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        if manifests.is_empty() {
            return Ok(manifests);
        }

        let first_manifest = manifests[0].clone();

        let group_manifests = self.group_by_spec(manifests);

        let mut merge_manifests = vec![];
        for (_spec_id, manifests) in group_manifests.into_iter().rev() {
            merge_manifests.extend(
                self.merge_group(snapshot_produce, &first_manifest, manifests)
                    .await?,
            );
        }

        Ok(merge_manifests)
    }
}

pub(crate) fn new_manifest_path(
    metadata_location: &str,
    meta_root_path: &str,
    commit_uuid: Uuid,
    manifest_counter: u64,
    format: DataFileFormat,
) -> String {
    format!("{metadata_location}/{meta_root_path}/{commit_uuid}-m{manifest_counter}.{format}")
}
