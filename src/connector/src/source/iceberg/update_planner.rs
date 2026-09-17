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
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, Literal, NameMapping, PartitionSpec, SchemaRef,
    Snapshot, SnapshotRef, Type,
};
use iceberg::table::Table;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::util::iter_util::ZipEqFast;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::update_reader::IcebergUpdateReadMode;
use crate::connector_common::{IcebergCommitKind, IcebergSourceContract};
use crate::sink::iceberg::validate_position_delete_descriptor;

const NAME_MAPPING: &str = "schema.name-mapping.default";

/// Persist this binding before bootstrap. Never rediscover the key or bootstrap snapshot on replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateBinding {
    table_uuid: Uuid,
    bootstrap_snapshot_id: Option<i64>,
    schema: SchemaRef,
    contract: IcebergSourceContract,
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
        let contract = IcebergSourceContract::from_properties(metadata.properties(), &schema)?
            .context("delete-aware source requires a table source contract")?;
        let binding = Self {
            table_uuid: metadata.uuid(),
            bootstrap_snapshot_id: metadata.current_snapshot_id(),
            schema,
            contract,
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
            IcebergSourceContract::from_properties(metadata.properties(), &self.schema)?.as_ref()
                == Some(&self.contract),
            "Iceberg source contract changed"
        );
        ensure!(
            read_name_mapping(table)? == self.name_mapping,
            "Iceberg name mapping changed"
        );
        self.contract
            .validate_projection(&self.schema, &self.project_field_ids)?;
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
        commit_kind(snapshot)?;
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

fn commit_kind(snapshot: &Snapshot) -> Result<IcebergCommitKind> {
    IcebergCommitKind::from_properties(&snapshot.summary().additional_properties)?
        .context("Iceberg snapshot is missing its source commit marker")
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
}

impl IcebergUpdateTask {
    pub fn validate_columns(&self, columns: &[ColumnCatalog]) -> Result<()> {
        self.binding.validate_columns(columns)
    }

    pub fn validate_key(&self, columns: &[ColumnCatalog], key_indices: &[usize]) -> Result<()> {
        self.validate_columns(columns)?;
        ensure!(
            key_indices
                .iter()
                .map(|index| self.binding.project_field_ids.get(*index).copied())
                .collect::<Option<Vec<_>>>()
                .as_deref()
                == Some(self.binding.contract.key_field_ids()),
            "Iceberg writer key differs from the planned source key"
        );
        Ok(())
    }

    pub fn record_count(&self) -> u64 {
        self.file.record_count
    }

    /// Reconstruct from the persisted descriptors, not from the latest snapshot's file membership.
    pub fn read_contract(
        &self,
        table: &Table,
    ) -> Result<(FileScanTask, IcebergSourceContract, IcebergUpdateReadMode)> {
        self.binding.validate(table)?;
        self.binding
            .ensure_ancestor(table, Some(self.id.snapshot_id))?;
        if let Some(parent) = self.parent_snapshot_id {
            self.binding.snapshot(table, parent)?;
        }
        ensure!(
            self.id.table_uuid == self.binding.table_uuid
                && self.id.data_file_path == self.file.path,
            "update task identity differs from its read contract"
        );
        let mode = match self.id.phase {
            IcebergUpdatePhase::Insert => IcebergUpdateReadMode::Insert,
            IcebergUpdatePhase::Delete => IcebergUpdateReadMode::Delete {
                parent_deletes: self.parent_deletes.clone(),
            },
        };
        Ok((
            self.file.scan_task(&self.binding)?,
            self.binding.contract.clone(),
            mode,
        ))
    }
}

/// Enumeration is complete when this plan is returned. An empty Delete list needs no Delete fence.
/// Persist the plan/tasks before handoff; this value alone does not provide exactly-once execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdatePlan {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub bootstrap: bool,
    pub kind: IcebergCommitKind,
    pub deletes: Vec<IcebergUpdateTask>,
    pub inserts: Vec<IcebergUpdateTask>,
}

pub struct IcebergUpdatePlanner {
    binding: Arc<IcebergUpdateBinding>,
    limits: IcebergUpdateLimits,
}

impl IcebergUpdatePlanner {
    pub fn new(binding: IcebergUpdateBinding) -> Self {
        Self {
            binding: Arc::new(binding),
            limits: IcebergUpdateLimits::default(),
        }
    }

    pub fn with_limits(mut self, limits: IcebergUpdateLimits) -> Result<Self> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Bootstrap imports visible rows even when its fixed snapshot is a compaction.
    pub async fn plan_bootstrap(&self, table: &Table) -> Result<Option<IcebergUpdatePlan>> {
        self.binding.validate(table)?;
        let Some(id) = self.binding.bootstrap_snapshot_id else {
            return Ok(None);
        };
        self.binding.ensure_ancestor(table, Some(id))?;
        let snapshot = self.binding.snapshot(table, id)?;
        let files = self.load_files(table, snapshot).await?;
        let mut plan = IcebergUpdatePlan {
            snapshot_id: id,
            parent_snapshot_id: None,
            bootstrap: true,
            kind: commit_kind(snapshot)?,
            deletes: vec![],
            inserts: vec![],
        };
        for file in files.into_values() {
            plan.inserts
                .push(self.task(&plan, file, IcebergUpdatePhase::Insert, vec![]));
        }
        Ok(Some(plan))
    }

    /// Returns only the direct successor of the acknowledged snapshot, never skipping ancestors.
    /// The caller must not use an enumerated-but-unapplied snapshot as `last_snapshot`.
    pub async fn plan_next(
        &self,
        table: &Table,
        last_snapshot: Option<i64>,
    ) -> Result<Option<IcebergUpdatePlan>> {
        self.binding.validate(table)?;
        ensure!(
            last_snapshot.is_some() || self.binding.bootstrap_snapshot_id.is_none(),
            "bootstrap must finish before incremental planning"
        );
        let Some(id) = self.binding.ensure_ancestor(table, last_snapshot)? else {
            return Ok(None);
        };
        let snapshot = self.binding.snapshot(table, id)?;
        let kind = commit_kind(snapshot)?;
        let current = self.load_files(table, snapshot).await?;
        let mut plan = IcebergUpdatePlan {
            snapshot_id: id,
            parent_snapshot_id: last_snapshot,
            bootstrap: false,
            kind,
            deletes: vec![],
            inserts: vec![],
        };
        if kind == IcebergCommitKind::Compaction {
            // Validate the new metadata baseline, without opening data/delete artifacts. The next
            // plan reloads this snapshot as its parent, so no stale file map survives compaction.
            return Ok(Some(plan));
        }
        let mut parent = match last_snapshot {
            Some(id) => {
                self.load_files(table, self.binding.snapshot(table, id)?)
                    .await?
            }
            None => BTreeMap::new(),
        };
        ensure!(
            parent.keys().all(|path| current.contains_key(path)),
            "ordinary data commit removed a whole data file"
        );
        for (path, file) in current {
            if let Some(mut old) = parent.remove(&path) {
                let parent_deletes = std::mem::replace(&mut old.deletes, file.deletes.clone());
                ensure!(old == file, "retained immutable data-file metadata changed");
                if parent_deletes != file.deletes {
                    plan.deletes.push(self.task(
                        &plan,
                        file,
                        IcebergUpdatePhase::Delete,
                        parent_deletes,
                    ));
                }
            } else {
                plan.inserts
                    .push(self.task(&plan, file, IcebergUpdatePhase::Insert, vec![]));
            }
        }
        Ok(Some(plan))
    }

    fn task(
        &self,
        plan: &IcebergUpdatePlan,
        file: UpdateFile,
        phase: IcebergUpdatePhase,
        parent_deletes: Vec<FileScanTaskDeleteFile>,
    ) -> IcebergUpdateTask {
        IcebergUpdateTask {
            id: IcebergUpdateTaskId {
                table_uuid: self.binding.table_uuid,
                snapshot_id: plan.snapshot_id,
                phase,
                data_file_path: file.path.clone(),
            },
            parent_snapshot_id: plan.parent_snapshot_id,
            binding: self.binding.clone(),
            file,
            parent_deletes,
        }
    }

    async fn load_files(
        &self,
        table: &Table,
        snapshot: &SnapshotRef,
    ) -> Result<BTreeMap<String, UpdateFile>> {
        let input = table.file_io().new_input(snapshot.manifest_list())?;
        ensure!(
            input.metadata().await?.size <= self.limits.max_metadata_bytes,
            "Iceberg manifest list exceeds source metadata limit"
        );
        let list = table
            .object_cache()
            .get_manifest_list(snapshot, &table.metadata_ref())
            .await?;
        let mut files = BTreeMap::new();
        let mut deletes = BTreeMap::new();
        for manifest_file in list.entries() {
            ensure!(
                u64::try_from(manifest_file.manifest_length)? <= self.limits.max_metadata_bytes,
                "Iceberg manifest exceeds source metadata limit"
            );
            ensure!(
                manifest_file.key_metadata.is_none(),
                "encrypted manifests are unsupported"
            );
            ensure!(
                table
                    .file_io()
                    .new_input(&manifest_file.manifest_path)?
                    .metadata()
                    .await?
                    .size
                    <= self.limits.max_metadata_bytes,
                "actual Iceberg manifest exceeds source metadata limit"
            );
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            let spec = table
                .metadata()
                .partition_spec_by_id(manifest_file.partition_spec_id)
                .context("manifest references a missing partition spec")?;
            ensure!(
                manifest.metadata().partition_spec == **spec,
                "manifest partition spec differs from table metadata"
            );
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
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
                match file.content_type() {
                    DataContentType::Data => {
                        ensure!(
                            file.file_format() == DataFileFormat::Parquet,
                            "update reader requires Parquet data files"
                        );
                        ensure!(
                            file.record_count() <= i64::MAX as u64,
                            "data record count exceeds position range"
                        );
                        let partition_type =
                            Type::Struct(spec.partition_type(&self.binding.schema)?);
                        let value = UpdateFile {
                            path: file.file_path().to_owned(),
                            size: file.file_size_in_bytes(),
                            record_count: file.record_count(),
                            sequence_number: sequence,
                            file_sequence_number: entry.file_sequence_number,
                            first_row_id: file.first_row_id(),
                            partition: Literal::Struct(file.partition().clone())
                                .try_into_json(&partition_type)?,
                            partition_spec: spec.clone(),
                            deletes: vec![],
                        };
                        ensure!(
                            files.insert(value.path.clone(), value).is_none(),
                            "duplicate live data-file path"
                        );
                        ensure!(
                            files.len() <= self.limits.max_files,
                            "Iceberg live file limit exceeded"
                        );
                    }
                    DataContentType::PositionDeletes => {
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
                        ensure!(
                            deletes.len() <= self.limits.max_files,
                            "Iceberg live delete limit exceeded"
                        );
                    }
                    DataContentType::EqualityDeletes => {
                        anyhow::bail!("equality deletes are unsupported")
                    }
                }
            }
        }
        for (path, delete) in deletes {
            // Dangling position deletes cannot affect any live data file.
            if let Some(file) = files.get_mut(&path) {
                ensure!(
                    delete.sequence_number >= file.sequence_number,
                    "delete predates its referenced data file"
                );
                file.deletes.push(delete);
            }
        }
        Ok(files)
    }
}

/// Explicit safety limits, not a constant-memory claim about the SDK's Avro decoder. A page
/// contains bounded tasks, while snapshot metadata is currently rebuilt under these limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergUpdateLimits {
    pub page_size: usize,
    pub max_page_bytes: usize,
    pub max_files: usize,
    pub max_metadata_bytes: u64,
}

impl Default for IcebergUpdateLimits {
    fn default() -> Self {
        Self {
            page_size: 32,
            max_page_bytes: 4 * 1024 * 1024,
            max_files: 100_000,
            max_metadata_bytes: 32 * 1024 * 1024,
        }
    }
}

impl IcebergUpdateLimits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.page_size > 0
                && self.max_page_bytes > 0
                && self.max_files > 0
                && self.max_metadata_bytes > 0,
            "Iceberg update limits must be positive"
        );
        Ok(())
    }
}

/// Exclusive file-path cursor within a fixed snapshot and phase. Advancing enumeration does not
/// acknowledge application. The List state machine keeps this cursor behind outstanding tasks.
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
        &self,
        table: &Table,
        cursor: &IcebergUpdateCursor,
    ) -> Result<IcebergUpdatePage> {
        self.limits.validate()?;
        let plan = if cursor.bootstrap {
            self.plan_bootstrap(table).await?
        } else {
            self.plan_next(table, cursor.parent_snapshot_id).await?
        }
        .context("pinned update snapshot no longer exists")?;
        ensure!(
            plan.snapshot_id == cursor.snapshot_id
                && plan.parent_snapshot_id == cursor.parent_snapshot_id,
            "update cursor no longer matches its fixed snapshots"
        );
        ensure!(
            !cursor.bootstrap || cursor.phase == IcebergUpdatePhase::Insert,
            "invalid bootstrap phase"
        );
        let tasks = match cursor.phase {
            IcebergUpdatePhase::Delete => plan.deletes,
            IcebergUpdatePhase::Insert => plan.inserts,
        };
        if let Some(path) = &cursor.after_path {
            ensure!(
                tasks.iter().any(|task| &task.id.data_file_path == path),
                "invalid exclusive update cursor"
            );
        }
        let mut remaining = tasks
            .into_iter()
            .filter(|task| {
                cursor
                    .after_path
                    .as_ref()
                    .is_none_or(|path| task.id.data_file_path > *path)
            })
            .peekable();
        let mut page = IcebergUpdatePage {
            phase_finished: false,
            tasks: vec![],
        };
        let mut bytes = 0;
        while let Some(task) = remaining.peek() {
            let size = serde_json::to_vec(task)?.len();
            ensure!(
                size <= self.limits.max_page_bytes,
                "single Iceberg update task exceeds page byte limit"
            );
            if page.tasks.len() == self.limits.page_size
                || bytes + size > self.limits.max_page_bytes
            {
                break;
            }
            bytes += size;
            let task = remaining.next().expect("peeked task");
            page.tasks.push(task);
        }
        page.phase_finished = remaining.peek().is_none();
        Ok(page)
    }
}

#[cfg(test)]
#[path = "update_planner_test.rs"]
mod tests;
