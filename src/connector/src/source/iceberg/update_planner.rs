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

//! Metadata-only planning of Insert and Delete updates. Plans do not advance ingestion
//! progress: the caller must durably finish Delete before Insert, then acknowledge the snapshot.
//! This is deliberately separate from the legacy append-only planner and List/Fetch protocol.

use std::collections::{BTreeMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use futures::stream::{BoxStream, Peekable};
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, Literal, Manifest, ManifestContentType,
    ManifestFile, NameMapping, Operation, PartitionSpec, SchemaRef, Snapshot, SnapshotRef, Type,
};
use iceberg::table::Table;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::util::iter_util::ZipEqFast;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::update_reader::IcebergUpdateReadMode;
use super::validate_update_projection;
use crate::sink::iceberg::validate_position_delete_descriptor;

const NAME_MAPPING: &str = "schema.name-mapping.default";

/// Persist this binding before bootstrap. Never rediscover the schema or bootstrap snapshot on replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateBinding {
    table_uuid: Uuid,
    bootstrap_snapshot_id: Option<i64>,
    schema: SchemaRef,
    project_field_ids: Vec<i32>,
    name_mapping: Option<Arc<NameMapping>>,
}

impl IcebergUpdateBinding {
    /// Resolve the List's downstream projection without dropping stored hidden key columns.
    pub fn bind_columns(table: &Table, columns: &[ColumnCatalog]) -> Result<Self> {
        let schema = table.metadata().current_schema();
        let field_ids = columns
            .iter()
            .map(|column| {
                schema
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|field| field.name == column.name())
                    .map(|field| field.id)
                    .with_context(|| format!("Iceberg stored column not found: {}", column.name()))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::bind(table, field_ids)
    }

    pub fn validate_columns(&self, columns: &[ColumnCatalog]) -> Result<()> {
        ensure!(
            self.project_field_ids.len() == columns.len()
                && self
                    .project_field_ids
                    .iter()
                    .zip_eq_fast(columns)
                    .all(|(id, column)| {
                        self.schema
                            .field_by_id(*id)
                            .is_some_and(|field| field.name == column.name())
                    }),
            "Iceberg stored projection differs from the source columns"
        );
        Ok(())
    }

    pub fn bind(table: &Table, project_field_ids: Vec<i32>) -> Result<Self> {
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let binding = Self {
            table_uuid: metadata.uuid(),
            bootstrap_snapshot_id: metadata.current_snapshot_id(),
            schema,
            project_field_ids,
            name_mapping: read_name_mapping(table)?,
        };
        binding.validate(table)?;
        if let Some(id) = binding.bootstrap_snapshot_id {
            binding.snapshot(table, id)?;
        }
        Ok(binding)
    }

    fn validate(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        ensure!(
            metadata.uuid() == self.table_uuid,
            "Iceberg table UUID changed"
        );
        ensure!(
            matches!(
                metadata.format_version(),
                FormatVersion::V2 | FormatVersion::V3
            ),
            "delete-aware source requires Iceberg V2 or V3"
        );
        ensure!(
            metadata.current_snapshot_id()
                == metadata
                    .snapshot_for_ref("main")
                    .map(|snapshot| snapshot.snapshot_id()),
            "delete-aware source requires the main branch"
        );
        ensure!(
            *metadata.current_schema() == self.schema,
            "Iceberg source schema changed"
        );
        ensure!(
            read_name_mapping(table)? == self.name_mapping,
            "Iceberg name mapping changed"
        );
        validate_update_projection(&self.schema, &self.project_field_ids)?;
        Ok(())
    }

    fn snapshot<'a>(&self, table: &'a Table, id: i64) -> Result<&'a SnapshotRef> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(id)
            .context("required Iceberg snapshot has expired or is missing")?;
        ensure!(
            snapshot.schema(table.metadata())? == self.schema,
            "Iceberg snapshot schema changed"
        );
        ensure!(
            snapshot.encryption_key_id().is_none(),
            "encrypted snapshots are unsupported"
        );
        Ok(snapshot)
    }

    /// Validate the complete remaining main ancestry, using links rather than snapshot-ID order.
    fn ensure_ancestor(&self, table: &Table, last: Option<i64>) -> Result<Option<i64>> {
        if let Some(id) = last {
            self.snapshot(table, id)?;
        }
        let mut cursor = table.metadata().current_snapshot_id();
        let mut next = None;
        let mut seen = HashSet::new();
        while cursor != last {
            let id =
                cursor.context("Iceberg history rolled back or diverged from source progress")?;
            ensure!(seen.insert(id), "cycle in Iceberg snapshot ancestry");
            let snapshot = self.snapshot(table, id)?;
            next = Some(id);
            cursor = snapshot.parent_snapshot_id();
        }
        Ok(next)
    }
}

fn read_name_mapping(table: &Table) -> Result<Option<Arc<NameMapping>>> {
    table
        .metadata()
        .properties()
        .get(NAME_MAPPING)
        .map(|value| {
            serde_json::from_str(value)
                .map(Arc::new)
                .map_err(Into::into)
        })
        .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergCommitKind {
    Data,
    Compaction,
}

impl IcebergCommitKind {
    fn from_operation(operation: &Operation) -> Self {
        match operation {
            Operation::Replace => Self::Compaction,
            Operation::Append | Operation::Overwrite | Operation::Delete => Self::Data,
        }
    }
}

fn commit_kind(snapshot: &Snapshot) -> IcebergCommitKind {
    IcebergCommitKind::from_operation(&snapshot.summary().operation)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcebergUpdatePhase {
    Delete,
    Insert,
}

/// Stable within a source job. The checkpoint assignment also includes job and generation identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergUpdateTaskId {
    pub table_uuid: Uuid,
    pub snapshot_id: i64,
    pub phase: IcebergUpdatePhase,
    pub data_file_path: String,
}

/// Unlike the legacy split, preserves partition constants, spec, sequence numbers and name mapping.
/// No predicates, virtual columns, or encryption keys are allowed in this read contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UpdateFile {
    path: String,
    size: u64,
    record_count: u64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
    first_row_id: Option<i64>,
    partition: serde_json::Value,
    partition_spec: Arc<PartitionSpec>,
    deletes: Vec<FileScanTaskDeleteFile>,
}

impl UpdateFile {
    fn scan_task(&self, binding: &IcebergUpdateBinding) -> Result<FileScanTask> {
        let partition_type = Type::Struct(self.partition_spec.partition_type(&binding.schema)?);
        let Some(Literal::Struct(partition)) =
            Literal::try_from_json(self.partition.clone(), &partition_type)?
        else {
            anyhow::bail!("invalid persisted update-task partition");
        };
        Ok(FileScanTask::builder()
            .with_file_size_in_bytes(self.size)
            .with_start(0)
            .with_length(self.size)
            .with_record_count(Some(self.record_count))
            .with_data_file_path(self.path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(binding.schema.clone())
            .with_project_field_ids(binding.project_field_ids.clone())
            .with_sequence_number(self.sequence_number)
            .with_data_sequence_number(Some(self.sequence_number))
            .with_file_sequence_number(self.file_sequence_number)
            .with_first_row_id(self.first_row_id)
            .with_partition(Some(partition))
            .with_partition_spec(Some(self.partition_spec.clone()))
            .with_name_mapping(binding.name_mapping.clone())
            .with_deletes(self.deletes.clone())
            .with_case_sensitive(true)
            .build())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateTask {
    pub id: IcebergUpdateTaskId,
    pub parent_snapshot_id: Option<i64>,
    binding: Arc<IcebergUpdateBinding>,
    file: UpdateFile,
    parent_deletes: Vec<FileScanTaskDeleteFile>,
    /// Legacy and mixed overwrite snapshots can remove complete data files.
    #[serde(default)]
    removed: bool,
}

impl IcebergUpdateTask {
    pub fn validate_columns(&self, columns: &[ColumnCatalog]) -> Result<()> {
        self.binding.validate_columns(columns)
    }

    pub fn validate_key(&self, columns: &[ColumnCatalog], key_indices: &[usize]) -> Result<()> {
        self.validate_columns(columns)?;
        let mut seen = HashSet::new();
        ensure!(
            !key_indices.is_empty()
                && key_indices
                    .iter()
                    .all(|index| *index < columns.len() && seen.insert(*index)),
            "Iceberg update source requires a valid SQL primary key"
        );
        Ok(())
    }

    pub fn record_count(&self) -> u64 {
        self.file.record_count
    }

    /// Validate the table and snapshot history, then build the scan task and read mode.
    /// Use persisted file descriptors, not the latest snapshot's file membership.
    pub fn prepare_read(&self, table: &Table) -> Result<(FileScanTask, IcebergUpdateReadMode)> {
        self.binding.validate(table)?;
        self.binding
            .ensure_ancestor(table, Some(self.id.snapshot_id))?;
        if let Some(parent) = self.parent_snapshot_id {
            self.binding.snapshot(table, parent)?;
        }
        ensure!(
            self.id.table_uuid == self.binding.table_uuid
                && self.id.data_file_path == self.file.path,
            "update task identity differs from its persisted descriptors"
        );
        let mode = match self.id.phase {
            IcebergUpdatePhase::Insert => IcebergUpdateReadMode::Insert,
            IcebergUpdatePhase::Delete if self.removed => IcebergUpdateReadMode::Remove,
            IcebergUpdatePhase::Delete => IcebergUpdateReadMode::Delete {
                parent_deletes: self.parent_deletes.clone(),
            },
        };
        Ok((self.file.scan_task(&self.binding)?, mode))
    }
}

/// Collected only by planner/reader tests. Production dispatches the lazy task stream in pages.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdatePlan {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub deletes: Vec<IcebergUpdateTask>,
    pub inserts: Vec<IcebergUpdateTask>,
}

pub struct IcebergUpdatePlanner {
    binding: Arc<IcebergUpdateBinding>,
    /// Runtime scan state, never checkpointed or shared with Fetch. Recovery reconstructs the
    /// deterministic stream and skips its dispatched prefix using the durable cursor.
    enumeration: Option<(
        IcebergUpdateCursor,
        Peekable<BoxStream<'static, Result<IcebergUpdateTask>>>,
    )>,
}

impl IcebergUpdatePlanner {
    pub fn new(binding: IcebergUpdateBinding) -> Self {
        Self {
            binding: Arc::new(binding),
            enumeration: None,
        }
    }

    #[cfg(test)]
    pub async fn plan_bootstrap(&self, table: &Table) -> Result<Option<IcebergUpdatePlan>> {
        let Some(cursor) = self.start_cursor(table, false, None)? else {
            return Ok(None);
        };
        self.collect_plan(table, cursor).await.map(Some)
    }

    #[cfg(test)]
    pub async fn plan_next(
        &self,
        table: &Table,
        last_snapshot: Option<i64>,
    ) -> Result<Option<IcebergUpdatePlan>> {
        ensure!(
            last_snapshot.is_some() || self.binding.bootstrap_snapshot_id.is_none(),
            "bootstrap must finish before incremental planning"
        );
        let Some(cursor) = self.start_cursor(table, true, last_snapshot)? else {
            return Ok(None);
        };
        self.collect_plan(table, cursor).await.map(Some)
    }

    #[cfg(test)]
    async fn collect_plan(
        &self,
        table: &Table,
        mut cursor: IcebergUpdateCursor,
    ) -> Result<IcebergUpdatePlan> {
        let mut plan = IcebergUpdatePlan {
            snapshot_id: cursor.snapshot_id,
            parent_snapshot_id: cursor.parent_snapshot_id,
            deletes: vec![],
            inserts: vec![],
        };
        if !cursor.bootstrap {
            plan.deletes = stream_tasks(self.binding.clone(), table.clone(), cursor.clone())
                .try_collect()
                .await?;
        }
        cursor.phase = IcebergUpdatePhase::Insert;
        plan.inserts = stream_tasks(self.binding.clone(), table.clone(), cursor)
            .try_collect()
            .await?;
        // Tests compare logical plans independently of manifest storage order.
        for tasks in [&mut plan.deletes, &mut plan.inserts] {
            tasks.sort_unstable_by(|left, right| {
                left.id.data_file_path.cmp(&right.id.data_file_path)
            });
        }
        Ok(plan)
    }
}

fn task(
    binding: &Arc<IcebergUpdateBinding>,
    cursor: &IcebergUpdateCursor,
    file: UpdateFile,
    parent_deletes: Vec<FileScanTaskDeleteFile>,
    removed: bool,
) -> IcebergUpdateTask {
    IcebergUpdateTask {
        id: IcebergUpdateTaskId {
            table_uuid: binding.table_uuid,
            snapshot_id: cursor.snapshot_id,
            phase: cursor.phase,
            data_file_path: file.path.clone(),
        },
        parent_snapshot_id: cursor.parent_snapshot_id,
        binding: binding.clone(),
        file,
        parent_deletes,
        removed,
    }
}

/// The parent is a lookup index, not a task plan. Current data manifests are consumed lazily;
/// retained files leave the index as they are visited, so its remainder identifies removals.
#[try_stream(boxed, ok = IcebergUpdateTask, error = anyhow::Error)]
async fn stream_tasks(
    binding: Arc<IcebergUpdateBinding>,
    table: Table,
    cursor: IcebergUpdateCursor,
) {
    let snapshot = binding.snapshot(&table, cursor.snapshot_id)?;
    let compaction = !cursor.bootstrap && commit_kind(snapshot) == IcebergCommitKind::Compaction;
    let mut parent: BTreeMap<String, UpdateFile> = if !cursor.bootstrap && !compaction {
        if let Some(id) = cursor.parent_snapshot_id {
            stream_files(binding.clone(), table.clone(), id)
                .map_ok(|file| (file.path.clone(), file))
                .try_collect()
                .await?
        } else {
            BTreeMap::new()
        }
    } else {
        BTreeMap::new()
    };
    let mut current = stream_files(binding.clone(), table, cursor.snapshot_id);
    while let Some(file) = current.try_next().await? {
        if compaction {
            // Validate all metadata, but never open row artifacts or emit maintenance events.
            continue;
        }
        if let Some(mut old) = parent.remove(&file.path) {
            let parent_deletes = std::mem::replace(&mut old.deletes, file.deletes.clone());
            ensure!(old == file, "retained immutable data-file metadata changed");
            if cursor.phase == IcebergUpdatePhase::Delete && parent_deletes != file.deletes {
                yield task(&binding, &cursor, file, parent_deletes, false);
            }
        } else if cursor.phase == IcebergUpdatePhase::Insert {
            yield task(&binding, &cursor, file, vec![], false);
        }
    }
    if cursor.phase == IcebergUpdatePhase::Delete && !compaction {
        for file in parent.into_values() {
            yield task(&binding, &cursor, file, vec![], true);
        }
    }
}

async fn load_manifest(
    binding: &IcebergUpdateBinding,
    table: &Table,
    file: &ManifestFile,
) -> Result<(Manifest, Arc<PartitionSpec>)> {
    ensure!(
        file.key_metadata.is_none(),
        "encrypted manifests are unsupported"
    );
    let manifest = file.load_manifest(table.file_io()).await?;
    let spec = table
        .metadata()
        .partition_spec_by_id(file.partition_spec_id)
        .context("manifest references a missing partition spec")?;
    ensure!(
        manifest.metadata().partition_spec == **spec,
        "manifest partition spec differs from table metadata"
    );
    // Resolve against the bound schema, including historical partition specs.
    spec.partition_type(&binding.schema)?;
    Ok((manifest, spec.clone()))
}

fn entry_sequence(entry: &iceberg::spec::ManifestEntry, snapshot: &Snapshot) -> Result<i64> {
    let file = entry.data_file();
    ensure!(
        file.key_metadata().is_none(),
        "encrypted files are unsupported"
    );
    ensure!(
        file.equality_ids().is_none(),
        "equality deletes are unsupported"
    );
    let sequence = entry
        .sequence_number()
        .context("missing file data sequence number")?;
    ensure!(
        sequence >= 0 && sequence <= snapshot.sequence_number(),
        "invalid file sequence number"
    );
    Ok(sequence)
}

/// Delete descriptors are indexed before data enumeration, since manifests are not ordered by
/// referenced data path. Only one data manifest and one task page are materialized at a time.
#[try_stream(boxed, ok = UpdateFile, error = anyhow::Error)]
async fn stream_files(binding: Arc<IcebergUpdateBinding>, table: Table, snapshot_id: i64) {
    let snapshot = binding.snapshot(&table, snapshot_id)?;
    let cache = table.object_cache();
    let metadata = table.metadata_ref();
    let list = cache.get_manifest_list(snapshot, &metadata).await?;
    let mut deletes = BTreeMap::new();
    for manifest_file in list
        .entries()
        .iter()
        .filter(|file| file.content == ManifestContentType::Deletes)
    {
        let (manifest, _) = load_manifest(&binding, &table, manifest_file).await?;
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let sequence = entry_sequence(entry, snapshot)?;
            let file = entry.data_file();
            ensure!(
                file.content_type() == DataContentType::PositionDeletes,
                "only position deletes are supported"
            );
            let referenced = file
                .referenced_data_file()
                .context("position deletes require a referenced data file")?;
            let delete = FileScanTaskDeleteFile::builder()
                .with_file_path(file.file_path().to_owned())
                .with_file_size_in_bytes(file.file_size_in_bytes())
                .with_file_type(DataContentType::PositionDeletes)
                .with_partition_spec_id(manifest_file.partition_spec_id)
                .with_file_format(file.file_format())
                .with_referenced_data_file(Some(referenced.clone()))
                .with_record_count(Some(file.record_count()))
                .with_content_offset(file.content_offset())
                .with_content_size_in_bytes(file.content_size_in_bytes())
                .with_sequence_number(sequence)
                .build();
            validate_position_delete_descriptor(&delete, &referenced)?;
            ensure!(
                deletes.insert(referenced, delete).is_none(),
                "multiple live delete artifacts for one data file"
            );
        }
    }
    let mut seen = HashSet::new();
    for manifest_file in list
        .entries()
        .iter()
        .filter(|file| file.content == ManifestContentType::Data)
    {
        let (manifest, spec) = load_manifest(&binding, &table, manifest_file).await?;
        let partition_type = Type::Struct(spec.partition_type(&binding.schema)?);
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let sequence = entry_sequence(entry, snapshot)?;
            let file = entry.data_file();
            ensure!(
                file.content_type() == DataContentType::Data,
                "non-data file in data manifest"
            );
            ensure!(
                file.file_format() == DataFileFormat::Parquet,
                "update reader requires Parquet data files"
            );
            ensure!(
                file.record_count() <= i64::MAX as u64,
                "data record count exceeds position range"
            );
            ensure!(
                seen.insert(file.file_path().to_owned()),
                "duplicate live data-file path"
            );
            let mut file_deletes = vec![];
            if let Some(delete) = deletes.remove(file.file_path()) {
                ensure!(
                    delete.sequence_number >= sequence,
                    "delete predates its referenced data file"
                );
                file_deletes.push(delete);
            }
            yield UpdateFile {
                path: file.file_path().to_owned(),
                size: file.file_size_in_bytes(),
                record_count: file.record_count(),
                sequence_number: sequence,
                file_sequence_number: entry.file_sequence_number,
                first_row_id: file.first_row_id(),
                partition: Literal::Struct(file.partition().clone())
                    .try_into_json(&partition_type)?,
                partition_spec: spec.clone(),
                deletes: file_deletes,
            };
        }
    }
    // Remaining descriptors reference data files that are no longer live.
}

/// Last dispatched file within a deterministic snapshot/phase stream, not a sorted path bound.
/// Recovery replays metadata up to this marker; normal pages retain the live manifest scan.
/// Advancing enumeration does not acknowledge application: List fences whole phases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergUpdateCursor {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub bootstrap: bool,
    pub phase: IcebergUpdatePhase,
    pub after_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdatePage {
    pub phase_finished: bool,
    pub tasks: Vec<IcebergUpdateTask>,
}

impl IcebergUpdatePlanner {
    pub fn start_cursor(
        &self,
        table: &Table,
        bootstrap_complete: bool,
        applied_snapshot: Option<i64>,
    ) -> Result<Option<IcebergUpdateCursor>> {
        self.binding.validate(table)?;
        let (id, parent, bootstrap) = if !bootstrap_complete {
            let Some(id) = self.binding.bootstrap_snapshot_id else {
                return Ok(None);
            };
            self.binding.ensure_ancestor(table, Some(id))?;
            (id, None, true)
        } else {
            let Some(id) = self.binding.ensure_ancestor(table, applied_snapshot)? else {
                return Ok(None);
            };
            (id, applied_snapshot, false)
        };
        Ok(Some(IcebergUpdateCursor {
            snapshot_id: id,
            parent_snapshot_id: parent,
            bootstrap,
            phase: if bootstrap {
                IcebergUpdatePhase::Insert
            } else {
                IcebergUpdatePhase::Delete
            },
            after_path: None,
        }))
    }

    pub async fn plan_page(
        &mut self,
        table: &Table,
        cursor: &IcebergUpdateCursor,
        batch_size: usize,
    ) -> Result<IcebergUpdatePage> {
        ensure!(batch_size > 0, "Iceberg fetch batch size must be positive");
        self.binding.validate(table)?;
        let expected = if cursor.bootstrap {
            self.binding
                .ensure_ancestor(table, self.binding.bootstrap_snapshot_id)?;
            self.binding.bootstrap_snapshot_id
        } else {
            self.binding
                .ensure_ancestor(table, cursor.parent_snapshot_id)?
        };
        ensure!(
            expected == Some(cursor.snapshot_id)
                && (!cursor.bootstrap || cursor.parent_snapshot_id.is_none()),
            "update cursor no longer matches its fixed snapshots"
        );
        ensure!(
            !cursor.bootstrap || cursor.phase == IcebergUpdatePhase::Insert,
            "invalid bootstrap phase"
        );

        // Taking the runtime scan makes errors/cancellation failure-atomic: the durable cursor
        // is unchanged and a retry reconstructs the stream instead of reusing partial progress.
        let mut remaining = match self.enumeration.take() {
            Some((previous, tasks)) if previous == *cursor => tasks,
            _ => {
                let mut tasks =
                    stream_tasks(self.binding.clone(), table.clone(), cursor.clone()).peekable();
                if let Some(path) = &cursor.after_path {
                    loop {
                        let task = tasks
                            .try_next()
                            .await?
                            .context("invalid exclusive update cursor")?;
                        if task.id.data_file_path == *path {
                            break;
                        }
                    }
                }
                tasks
            }
        };
        let mut tasks = Vec::with_capacity(batch_size);
        while tasks.len() < batch_size {
            let Some(task) = remaining.try_next().await? else {
                break;
            };
            tasks.push(task);
        }
        // Keep at most one lookahead task, not a complete snapshot plan.
        let phase_finished = match Pin::new(&mut remaining).peek().await {
            None => true,
            Some(Ok(_)) => false,
            Some(Err(_)) => {
                return Err(remaining.next().await.expect("peeked error").unwrap_err());
            }
        };
        let mut next = cursor.clone();
        if let Some(task) = tasks.last() {
            next.after_path = Some(task.id.data_file_path.clone());
        }
        self.enumeration = Some((next, remaining));
        Ok(IcebergUpdatePage {
            phase_finished,
            tasks,
        })
    }
}

#[cfg(test)]
#[path = "update_planner_test.rs"]
mod tests;
