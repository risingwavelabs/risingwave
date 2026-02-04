// Copyright 2024 RisingWave Labs
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

pub mod parquet_file_handler;

mod metrics;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use iceberg::Catalog;
use iceberg::expr::{BoundPredicate, Predicate as IcebergPredicate};
use iceberg::scan::FileScanTask;
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
pub use parquet_file_handler::*;
use phf::{Set, phf_set};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayImpl, DataChunk, I64Array, Utf8Array};
use risingwave_common::bail;
use risingwave_common::types::JsonbVal;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;
use serde::{Deserialize, Serialize};

pub use self::metrics::{GLOBAL_ICEBERG_SCAN_METRICS, IcebergScanMetrics};
use crate::connector_common::{IcebergCommon, IcebergTableIdentifier};
use crate::enforce_secret::{EnforceSecret, EnforceSecretError};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceProperties,
    SplitEnumerator, SplitId, SplitMetaData, SplitReader, UnknownFields,
};
pub const ICEBERG_CONNECTOR: &str = "iceberg";

#[derive(Clone, Debug, Deserialize, with_options::WithOptions)]
pub struct IcebergProperties {
    #[serde(flatten)]
    pub common: IcebergCommon,

    #[serde(flatten)]
    pub table: IcebergTableIdentifier,

    // For jdbc catalog
    #[serde(rename = "catalog.jdbc.user")]
    pub jdbc_user: Option<String>,
    #[serde(rename = "catalog.jdbc.password")]
    pub jdbc_password: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl EnforceSecret for IcebergProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "catalog.jdbc.password",
    };

    fn enforce_secret<'a>(prop_iter: impl Iterator<Item = &'a str>) -> ConnectorResult<()> {
        for prop in prop_iter {
            IcebergCommon::enforce_one(prop)?;
            if Self::ENFORCE_SECRET_PROPERTIES.contains(prop) {
                return Err(EnforceSecretError {
                    key: prop.to_owned(),
                }
                .into());
            }
        }
        Ok(())
    }
}

impl IcebergProperties {
    pub async fn create_catalog(&self) -> ConnectorResult<Arc<dyn Catalog>> {
        let mut java_catalog_props = HashMap::new();
        if let Some(jdbc_user) = self.jdbc_user.clone() {
            java_catalog_props.insert("jdbc.user".to_owned(), jdbc_user);
        }
        if let Some(jdbc_password) = self.jdbc_password.clone() {
            java_catalog_props.insert("jdbc.password".to_owned(), jdbc_password);
        }
        // TODO: support path_style_access and java_catalog_props for iceberg source
        self.common.create_catalog(&java_catalog_props).await
    }

    pub async fn load_table(&self) -> ConnectorResult<Table> {
        let mut java_catalog_props = HashMap::new();
        if let Some(jdbc_user) = self.jdbc_user.clone() {
            java_catalog_props.insert("jdbc.user".to_owned(), jdbc_user);
        }
        if let Some(jdbc_password) = self.jdbc_password.clone() {
            java_catalog_props.insert("jdbc.password".to_owned(), jdbc_password);
        }
        // TODO: support java_catalog_props for iceberg source
        self.common
            .load_table(&self.table, &java_catalog_props)
            .await
    }
}

impl SourceProperties for IcebergProperties {
    type Split = IcebergSplit;
    type SplitEnumerator = IcebergSplitEnumerator;
    type SplitReader = IcebergFileReader;

    const SOURCE_NAME: &'static str = ICEBERG_CONNECTOR;
}

impl UnknownFields for IcebergProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct IcebergFileScanTaskJsonStr(String);

impl IcebergFileScanTaskJsonStr {
    pub fn deserialize(&self) -> FileScanTask {
        serde_json::from_str(&self.0).unwrap()
    }

    pub fn serialize(task: &FileScanTask) -> Self {
        Self(serde_json::to_string(task).unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IcebergFileScanTask {
    Data(Vec<FileScanTask>),
    EqualityDelete(Vec<FileScanTask>),
    PositionDelete(Vec<FileScanTask>),
}

impl IcebergFileScanTask {
    pub fn tasks(&self) -> &[FileScanTask] {
        match self {
            IcebergFileScanTask::Data(file_scan_tasks)
            | IcebergFileScanTask::EqualityDelete(file_scan_tasks)
            | IcebergFileScanTask::PositionDelete(file_scan_tasks) => file_scan_tasks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks().is_empty()
    }

    pub fn files(&self) -> Vec<String> {
        self.tasks()
            .iter()
            .map(|task| task.data_file_path.clone())
            .collect()
    }

    pub fn predicate(&self) -> Option<&BoundPredicate> {
        let first_task = self.tasks().first()?;
        first_task.predicate.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSplit {
    pub split_id: i64,
    pub task: IcebergFileScanTask,
}

impl IcebergSplit {
    pub fn empty(iceberg_scan_type: IcebergScanType) -> Self {
        let task = match iceberg_scan_type {
            IcebergScanType::DataScan => IcebergFileScanTask::Data(vec![]),
            IcebergScanType::EqualityDeleteScan => IcebergFileScanTask::EqualityDelete(vec![]),
            IcebergScanType::PositionDeleteScan => IcebergFileScanTask::PositionDelete(vec![]),
            _ => unimplemented!(),
        };
        Self { split_id: 0, task }
    }
}

impl SplitMetaData for IcebergSplit {
    fn id(&self) -> SplitId {
        self.split_id.to_string().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e).into())
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSplitEnumerator {
    config: IcebergProperties,
}

#[derive(Debug, Clone)]
pub struct IcebergDeleteParameters {
    pub equality_delete_columns: Vec<String>,
    pub has_position_delete: bool,
    pub snapshot_id: Option<i64>,
}

#[async_trait]
impl SplitEnumerator for IcebergSplitEnumerator {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self::new_inner(properties, context))
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        // Like file source, iceberg streaming source has a List Executor and a Fetch Executor,
        // instead of relying on SplitEnumerator on meta.
        // TODO: add some validation logic here.
        Ok(vec![])
    }
}
impl IcebergSplitEnumerator {
    pub fn new_inner(properties: IcebergProperties, _context: SourceEnumeratorContextRef) -> Self {
        Self { config: properties }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IcebergTimeTravelInfo {
    Version(i64),
    TimestampMs(i64),
}

#[derive(Debug, Clone)]
pub struct IcebergListResult {
    pub data_files: Vec<FileScanTask>,
    pub equality_delete_files: Vec<FileScanTask>,
    pub position_delete_files: Vec<FileScanTask>,
    pub equality_delete_columns: Vec<String>,
    pub format_version: FormatVersion,
}

impl IcebergSplitEnumerator {
    pub fn get_snapshot_id(
        table: &Table,
        time_travel_info: Option<IcebergTimeTravelInfo>,
    ) -> ConnectorResult<Option<i64>> {
        let current_snapshot = table.metadata().current_snapshot();
        let Some(current_snapshot) = current_snapshot else {
            return Ok(None);
        };

        let snapshot_id = match time_travel_info {
            Some(IcebergTimeTravelInfo::Version(version)) => {
                let Some(snapshot) = table.metadata().snapshot_by_id(version) else {
                    bail!("Cannot find the snapshot id in the iceberg table.");
                };
                snapshot.snapshot_id()
            }
            Some(IcebergTimeTravelInfo::TimestampMs(timestamp)) => {
                let snapshot = table
                    .metadata()
                    .snapshots()
                    .filter(|snapshot| snapshot.timestamp_ms() <= timestamp)
                    .max_by_key(|snapshot| snapshot.timestamp_ms());
                match snapshot {
                    Some(snapshot) => snapshot.snapshot_id(),
                    None => {
                        // convert unix time to human-readable time
                        let time = chrono::DateTime::from_timestamp_millis(timestamp);
                        if let Some(time) = time {
                            tracing::warn!("Cannot find a snapshot older than {}", time);
                        } else {
                            tracing::warn!("Cannot find a snapshot");
                        }
                        return Ok(None);
                    }
                }
            }
            None => current_snapshot.snapshot_id(),
        };
        Ok(Some(snapshot_id))
    }

    pub async fn list_scan_tasks(
        &self,
        time_travel_info: Option<IcebergTimeTravelInfo>,
        predicate: IcebergPredicate,
    ) -> ConnectorResult<Option<IcebergListResult>> {
        let table = self.config.load_table().await?;
        let snapshot_id = Self::get_snapshot_id(&table, time_travel_info)?;

        let Some(snapshot_id) = snapshot_id else {
            return Ok(None);
        };
        let res = self
            .list_scan_tasks_inner(&table, snapshot_id, predicate)
            .await?;
        Ok(Some(res))
    }

    async fn list_scan_tasks_inner(
        &self,
        table: &Table,
        snapshot_id: i64,
        predicate: IcebergPredicate,
    ) -> ConnectorResult<IcebergListResult> {
        let format_version = table.metadata().format_version();
        let table_schema = table.metadata().current_schema();
        tracing::debug!("iceberg_table_schema: {:?}", table_schema);

        let mut position_delete_files = vec![];
        let mut position_delete_files_set = HashSet::new();
        let mut data_files = vec![];
        let mut equality_delete_files = vec![];
        let mut equality_delete_files_set = HashSet::new();
        let mut equality_delete_ids = None;
        let mut scan_builder = table.scan().snapshot_id(snapshot_id).select_all();
        if predicate != IcebergPredicate::AlwaysTrue {
            scan_builder = scan_builder.with_filter(predicate.clone());
        }
        let scan = scan_builder.build()?;
        let file_scan_stream = scan.plan_files().await?;

        #[for_await]
        for task in file_scan_stream {
            let task: FileScanTask = task?;

            // Collect delete files for separate scan types, but keep task.deletes intact
            for delete_file in &task.deletes {
                let delete_file = delete_file.as_ref().clone();
                match delete_file.data_file_content {
                    iceberg::spec::DataContentType::Data => {
                        bail!("Data file should not in task deletes");
                    }
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        if equality_delete_files_set.insert(delete_file.data_file_path.clone()) {
                            if equality_delete_ids.is_none() {
                                equality_delete_ids = delete_file.equality_ids.clone();
                            } else if equality_delete_ids != delete_file.equality_ids {
                                bail!(
                                    "The schema of iceberg equality delete file must be consistent"
                                );
                            }
                            equality_delete_files.push(delete_file);
                        }
                    }
                    iceberg::spec::DataContentType::PositionDeletes => {
                        if position_delete_files_set.insert(delete_file.data_file_path.clone()) {
                            position_delete_files.push(delete_file);
                        }
                    }
                }
            }

            match task.data_file_content {
                iceberg::spec::DataContentType::Data => {
                    // Keep the original task with its deletes field intact
                    data_files.push(task);
                }
                iceberg::spec::DataContentType::EqualityDeletes => {
                    bail!("Equality delete files should not be in the data files");
                }
                iceberg::spec::DataContentType::PositionDeletes => {
                    bail!("Position delete files should not be in the data files");
                }
            }
        }
        let schema = scan
            .snapshot()
            .context("snapshot not found")?
            .schema(table.metadata())?;
        let equality_delete_columns = equality_delete_ids
            .unwrap_or_default()
            .into_iter()
            .map(|id| match schema.name_by_field_id(id) {
                Some(name) => Ok::<std::string::String, ConnectorError>(name.to_owned()),
                None => bail!("Delete field id {} not found in schema", id),
            })
            .collect::<ConnectorResult<Vec<_>>>()?;

        Ok(IcebergListResult {
            data_files,
            equality_delete_files,
            position_delete_files,
            equality_delete_columns,
            format_version,
        })
    }

    /// Uniformly distribute scan tasks to compute nodes.
    /// It's deterministic so that it can best utilize the data locality.
    ///
    /// # Arguments
    /// * `file_scan_tasks`: The file scan tasks to be split.
    /// * `split_num`: The number of splits to be created.
    ///
    /// This algorithm is based on a min-heap. It will push all groups into the heap, and then pop the smallest group and add the file scan task to it.
    /// Ensure that the total length of each group is as balanced as possible.
    /// The time complexity is O(n log k), where n is the number of file scan tasks and k is the number of splits.
    /// The space complexity is O(k), where k is the number of splits.
    /// The algorithm is stable, so the order of the file scan tasks will be preserved.
    pub fn split_n_vecs(
        file_scan_tasks: Vec<FileScanTask>,
        split_num: usize,
    ) -> Vec<Vec<FileScanTask>> {
        use std::cmp::{Ordering, Reverse};

        #[derive(Default)]
        struct FileScanTaskGroup {
            idx: usize,
            tasks: Vec<FileScanTask>,
            total_length: u64,
        }

        impl Ord for FileScanTaskGroup {
            fn cmp(&self, other: &Self) -> Ordering {
                // when total_length is the same, we will sort by index
                if self.total_length == other.total_length {
                    self.idx.cmp(&other.idx)
                } else {
                    self.total_length.cmp(&other.total_length)
                }
            }
        }

        impl PartialOrd for FileScanTaskGroup {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Eq for FileScanTaskGroup {}

        impl PartialEq for FileScanTaskGroup {
            fn eq(&self, other: &Self) -> bool {
                self.total_length == other.total_length
            }
        }

        let mut heap = BinaryHeap::new();
        // push all groups into heap
        for idx in 0..split_num {
            heap.push(Reverse(FileScanTaskGroup {
                idx,
                tasks: vec![],
                total_length: 0,
            }));
        }

        for file_task in file_scan_tasks {
            let mut group = heap.peek_mut().unwrap();
            group.0.total_length += file_task.length;
            group.0.tasks.push(file_task);
        }

        // convert heap into vec and extract tasks
        heap.into_vec()
            .into_iter()
            .map(|reverse_group| reverse_group.0.tasks)
            .collect()
    }
}

pub struct IcebergScanOpts {
    pub chunk_size: usize,
    pub need_seq_num: bool,
    pub need_file_path_and_pos: bool,
    pub handle_delete_files: bool,
}

/// Scan a data file. Delete files are handled by the iceberg-rust `reader.read` implementation.
#[try_stream(ok = DataChunk, error = ConnectorError)]
pub async fn scan_task_to_chunk_with_deletes(
    table: Table,
    mut data_file_scan_task: FileScanTask,
    IcebergScanOpts {
        chunk_size,
        need_seq_num,
        need_file_path_and_pos,
        handle_delete_files,
    }: IcebergScanOpts,
    metrics: Option<Arc<IcebergScanMetrics>>,
) {
    let table_name = table.identifier().name().to_owned();

    let mut read_bytes = scopeguard::guard(0, |read_bytes| {
        if let Some(metrics) = metrics.clone() {
            metrics
                .iceberg_read_bytes
                .with_guarded_label_values(&[&table_name])
                .inc_by(read_bytes as _);
        }
    });

    let data_file_path = data_file_scan_task.data_file_path.clone();
    let data_sequence_number = data_file_scan_task.sequence_number;

    tracing::debug!(
        "scan_task_to_chunk_with_deletes: data_file={}, handle_delete_files={}, total_delete_files={}",
        data_file_path,
        handle_delete_files,
        data_file_scan_task.deletes.len()
    );

    if !handle_delete_files {
        // Keep the delete files from being applied when the caller opts out.
        data_file_scan_task.deletes.clear();
    }

    // Read the data file; delete application is delegated to the reader.
    let reader = table
        .reader_builder()
        .with_batch_size(chunk_size)
        .with_row_group_filtering_enabled(true)
        .with_row_selection_enabled(true)
        .build();
    let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task));

    let record_batch_stream: iceberg::scan::ArrowRecordBatchStream =
        reader.read(Box::pin(file_scan_stream))?;
    let mut record_batch_stream = record_batch_stream.enumerate();

    // Process each record batch. Delete application is handled by the SDK.
    while let Some((batch_index, record_batch)) = record_batch_stream.next().await {
        let record_batch = record_batch?;
        let batch_start_pos = (batch_index * chunk_size) as i64;

        let mut chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
        let row_count = chunk.capacity();

        // Add metadata columns if requested
        if need_seq_num {
            let (mut columns, visibility) = chunk.into_parts();
            columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(
                std::iter::repeat_n(data_sequence_number, row_count),
            ))));
            chunk = DataChunk::from_parts(columns.into(), visibility);
        }

        if need_file_path_and_pos {
            let (mut columns, visibility) = chunk.into_parts();
            columns.push(Arc::new(ArrayImpl::Utf8(Utf8Array::from_iter(
                std::iter::repeat_n(data_file_path.as_str(), row_count),
            ))));

            // Generate position values for each row in the batch
            let positions: Vec<i64> =
                (batch_start_pos..(batch_start_pos + row_count as i64)).collect();
            columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(positions))));

            chunk = DataChunk::from_parts(columns.into(), visibility);
        }

        *read_bytes += chunk.estimated_heap_size() as u64;
        yield chunk;
    }
}

#[derive(Debug)]
pub struct IcebergFileReader {}

#[async_trait]
impl SplitReader for IcebergFileReader {
    type Properties = IcebergProperties;
    type Split = IcebergSplit;

    async fn new(
        _props: IcebergProperties,
        _splits: Vec<IcebergSplit>,
        _parser_config: ParserConfig,
        _source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        unimplemented!()
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::scan::FileScanTask;
    use iceberg::spec::{DataContentType, Schema};

    use super::*;

    fn create_file_scan_task(length: u64, id: u64) -> FileScanTask {
        FileScanTask {
            length,
            start: 0,
            record_count: Some(0),
            data_file_path: format!("test_{}.parquet", id),
            data_file_content: DataContentType::Data,
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            file_size_in_bytes: 0,
            partition: None,
            partition_spec: None,
            name_mapping: None,
        }
    }

    #[test]
    fn test_split_n_vecs_basic() {
        let file_scan_tasks = (1..=12)
            .map(|i| create_file_scan_task(i + 100, i))
            .collect::<Vec<_>>(); // Ensure the correct function is called

        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 3);

        assert_eq!(groups.len(), 3);

        let group_lengths: Vec<u64> = groups
            .iter()
            .map(|group| group.iter().map(|task| task.length).sum())
            .collect();

        let max_length = *group_lengths.iter().max().unwrap();
        let min_length = *group_lengths.iter().min().unwrap();
        assert!(max_length - min_length <= 10, "Groups should be balanced");

        let total_tasks: usize = groups.iter().map(|group| group.len()).sum();
        assert_eq!(total_tasks, 12);
    }

    #[test]
    fn test_split_n_vecs_empty() {
        let file_scan_tasks = Vec::new();
        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.is_empty()));
    }

    #[test]
    fn test_split_n_vecs_single_task() {
        let file_scan_tasks = vec![create_file_scan_task(100, 1)];
        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.iter().filter(|group| !group.is_empty()).count(), 1);
    }

    #[test]
    fn test_split_n_vecs_uneven_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(1000, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
        ];

        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks, 2);
        assert_eq!(groups.len(), 2);

        let group_with_large_task = groups
            .iter()
            .find(|group| group.iter().any(|task| task.length == 1000))
            .unwrap();
        assert_eq!(group_with_large_task.len(), 1);
    }

    #[test]
    fn test_split_n_vecs_same_files_distribution() {
        let file_scan_tasks = vec![
            create_file_scan_task(100, 1),
            create_file_scan_task(100, 2),
            create_file_scan_task(100, 3),
            create_file_scan_task(100, 4),
            create_file_scan_task(100, 5),
            create_file_scan_task(100, 6),
            create_file_scan_task(100, 7),
            create_file_scan_task(100, 8),
        ];

        let groups = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks.clone(), 4)
            .iter()
            .map(|g| {
                g.iter()
                    .map(|task| task.data_file_path.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for _ in 0..10000 {
            let groups_2 = IcebergSplitEnumerator::split_n_vecs(file_scan_tasks.clone(), 4)
                .iter()
                .map(|g| {
                    g.iter()
                        .map(|task| task.data_file_path.clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            assert_eq!(groups, groups_2);
        }
    }
}
