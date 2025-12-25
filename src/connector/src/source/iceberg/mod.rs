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

pub mod parquet_file_handler;

mod metrics;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use iceberg::Catalog;
use iceberg::expr::Predicate as IcebergPredicate;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataContentType;
use iceberg::table::Table;
use itertools::Itertools;
pub use parquet_file_handler::*;
use phf::{Set, phf_set};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayImpl, DataChunk, I64Array, Utf8Array};
use risingwave_common::bail;
use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME, ROW_ID_COLUMN_NAME, Schema
};
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
    CountStar(u64),
}

impl Hash for IcebergFileScanTask {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            IcebergFileScanTask::Data(file_scan_tasks) => {
                file_scan_tasks.iter().for_each(|task| {
                    task.data_file_path.hash(state);
                });
            }
            IcebergFileScanTask::EqualityDelete(file_scan_tasks) => {
                file_scan_tasks.iter().for_each(|task| {
                    task.data_file_path.hash(state);
                });
            }
            IcebergFileScanTask::PositionDelete(file_scan_tasks) => {
                file_scan_tasks.iter().for_each(|task| {
                    task.data_file_path.hash(state);
                });
            }
            IcebergFileScanTask::CountStar(count) => {
                count.hash(state);
            }
        }
    }
}

impl IcebergFileScanTask {
    pub fn new_count_star(count_sum: u64) -> Self {
        IcebergFileScanTask::CountStar(count_sum)
    }

    pub fn get_iceberg_scan_type(&self) -> IcebergScanType {
        match self {
            IcebergFileScanTask::Data(_) => IcebergScanType::DataScan,
            IcebergFileScanTask::EqualityDelete(_) => IcebergScanType::EqualityDeleteScan,
            IcebergFileScanTask::PositionDelete(_) => IcebergScanType::PositionDeleteScan,
            IcebergFileScanTask::CountStar(_) => IcebergScanType::CountStar,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            IcebergFileScanTask::Data(data_files) => data_files.is_empty(),
            IcebergFileScanTask::EqualityDelete(equality_delete_files) => {
                equality_delete_files.is_empty()
            }
            IcebergFileScanTask::PositionDelete(position_delete_files) => {
                position_delete_files.is_empty()
            }
            IcebergFileScanTask::CountStar(_) => false,
        }
    }

    pub fn files(&self) -> Vec<String> {
        match self {
            IcebergFileScanTask::Data(file_scan_tasks)
            | IcebergFileScanTask::EqualityDelete(file_scan_tasks)
            | IcebergFileScanTask::PositionDelete(file_scan_tasks) => file_scan_tasks
                .iter()
                .map(|task| task.data_file_path.clone())
                .collect(),
            IcebergFileScanTask::CountStar(_) => vec![],
        }
    }

    pub fn add_project_field_ids(&mut self, project_field_ids: Vec<i32>) {
        match self {
            IcebergFileScanTask::Data(file_scan_tasks)
            | IcebergFileScanTask::EqualityDelete(file_scan_tasks)
            | IcebergFileScanTask::PositionDelete(file_scan_tasks) => {
                for task in file_scan_tasks.iter_mut() {
                    task.project_field_ids = project_field_ids.clone();
                }
            }
            IcebergFileScanTask::CountStar(_) => {}
        }
    }

    /// Splits the current `IcebergFileScanTask` into multiple smaller tasks.
    ///
    /// # Parameters
    /// - `split_num`: The number of splits to create. The scan tasks will be divided as evenly as possible among the splits.
    ///
    /// # Returns
    /// Returns a `ConnectorResult` containing a vector of split `IcebergFileScanTask`s.
    /// For `CountStar` variant, returns a single-element vector containing itself.
    pub fn split(self, split_num: usize) -> ConnectorResult<Vec<IcebergFileScanTask>> {
        match self {
            IcebergFileScanTask::Data(file_scan_tasks) => {
                let split_tasks = Self::split_n_vecs(file_scan_tasks, split_num);
                Ok(split_tasks
                    .into_iter()
                    .map(IcebergFileScanTask::Data)
                    .collect())
            }
            IcebergFileScanTask::EqualityDelete(file_scan_tasks) => {
                let split_tasks = Self::split_n_vecs(file_scan_tasks, split_num);
                Ok(split_tasks
                    .into_iter()
                    .map(IcebergFileScanTask::EqualityDelete)
                    .collect())
            }
            IcebergFileScanTask::PositionDelete(file_scan_tasks) => {
                let split_tasks = Self::split_n_vecs(file_scan_tasks, split_num);
                Ok(split_tasks
                    .into_iter()
                    .map(IcebergFileScanTask::PositionDelete)
                    .collect())
            }
            IcebergFileScanTask::CountStar(count) => {
                Ok(vec![IcebergFileScanTask::CountStar(count)])
            }
        }
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
    fn split_n_vecs(
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSplit {
    pub split_id: i64,
    pub snapshot_id: i64,
    pub task: IcebergFileScanTask,
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
pub struct IcebergTaskParameters {
    pub data_tasks: IcebergFileScanTask,
    pub equality_delete_tasks: IcebergFileScanTask,
    pub equality_delete_columns: Vec<String>,
    pub position_delete_tasks: IcebergFileScanTask,
    pub count: u64,
    pub snapshot_id: Option<i64>,
    pub name_to_field_id: HashMap<String, i32>,
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

pub enum IcebergTimeTravelInfo {
    Version(i64),
    TimestampMs(i64),
}

impl IcebergSplitEnumerator {
    pub fn get_snapshot_id(
        table: &Table,
        time_travel_info: Option<IcebergTimeTravelInfo>,
    ) -> ConnectorResult<Option<i64>> {
        let current_snapshot = table.metadata().current_snapshot();
        if current_snapshot.is_none() {
            return Ok(None);
        }

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
                            bail!("Cannot find a snapshot older than {}", time);
                        } else {
                            bail!("Cannot find a snapshot");
                        }
                    }
                }
            }
            None => {
                assert!(current_snapshot.is_some());
                current_snapshot.unwrap().snapshot_id()
            }
        };
        Ok(Some(snapshot_id))
    }

    pub async fn get_iceberg_task_parameters(
        &self,
        schema: Schema,
        predicate: IcebergPredicate,
        time_travel_info: Option<IcebergTimeTravelInfo>,
    ) -> ConnectorResult<IcebergTaskParameters> {
        let table = self.config.load_table().await?;
        let snapshot_id = Self::get_snapshot_id(&table, time_travel_info)?;
        let snapshot_id = match snapshot_id {
            Some(id) => id,
            None => {
                return Ok(IcebergTaskParameters {
                    data_tasks: IcebergFileScanTask::Data(vec![]),
                    equality_delete_tasks: IcebergFileScanTask::EqualityDelete(vec![]),
                    equality_delete_columns: vec![],
                    position_delete_tasks: IcebergFileScanTask::PositionDelete(vec![]),
                    count: 0,
                    snapshot_id: None,
                    name_to_field_id: HashMap::default(),
                });
            }
        };
        let table_schema = table.metadata().current_schema();
        tracing::debug!("iceberg_table_schema: {:?}", table_schema);
        let schema_names = schema.names();
        let require_names = schema_names
            .iter()
            .filter(|name| {
                name.ne(&ICEBERG_SEQUENCE_NUM_COLUMN_NAME)
                    && name.ne(&ICEBERG_FILE_PATH_COLUMN_NAME)
                    && name.ne(&ICEBERG_FILE_POS_COLUMN_NAME)
                    && name.ne(&ROW_ID_COLUMN_NAME)
            })
            .cloned()
            .collect_vec();
        let name_to_field_id: HashMap<String, i32> = require_names
            .iter()
            .filter_map(|name| {
                match table_schema.field_id_by_name(name) {
                    Some(field_id) => Some((name.clone(), field_id)),
                    None => None,
                }
            })
            .collect();

        let scan = table
            .scan()
            .with_filter(predicate)
            .snapshot_id(snapshot_id)
            .select(require_names)
            .build()
            .map_err(|e| anyhow!(e))?;
        let schema = scan
            .snapshot()
            .context("snapshot not found")?
            .schema(table.metadata())?;
        let file_scan_stream = scan.plan_files().await.map_err(|e| anyhow!(e))?;

        let mut data_file_scan_tasks = vec![];
        let mut equality_delete_set = HashSet::new();
        let mut equality_delete_file_scan_tasks = vec![];
        let mut position_delete_set = HashSet::new();
        let mut position_delete_file_scan_tasks = vec![];
        let mut equality_ids: Option<Vec<i32>> = None;
        let mut count_sum = 0u64;

        #[for_await]
        for task in file_scan_stream {
            let task: FileScanTask = task.map_err(|e| anyhow!(e))?;
            count_sum += task
                .record_count
                .ok_or_else(|| anyhow!("record count must be present for count star"))?;
            for delete in &task.deletes {
                match delete.data_file_content {
                    DataContentType::EqualityDeletes => {
                        if equality_ids.is_none() {
                            equality_ids = delete.equality_ids.clone();
                        } else if equality_ids != delete.equality_ids {
                            bail!("The schema of iceberg equality delete file must be consistent");
                        }
                        if !equality_delete_set.contains(&delete.data_file_path) {
                            equality_delete_set.insert(delete.data_file_path.clone());
                            let mut delete_file_scan_task = delete.as_ref().clone();
                            delete_file_scan_task.project_field_ids = delete_file_scan_task.equality_ids.clone().unwrap_or_default();
                            equality_delete_file_scan_tasks.push(delete_file_scan_task);
                        }
                    }
                    DataContentType::PositionDeletes => {
                        if !position_delete_set.contains(&delete.data_file_path) {
                            position_delete_set.insert(delete.data_file_path.clone());
                            position_delete_file_scan_tasks.push(delete.as_ref().clone());
                        }
                    }
                    _ => {
                        bail!(
                            "Unexpected delete file type in data scan: {:?}",
                            delete.data_file_content
                        )
                    }
                }
            }
            data_file_scan_tasks.push(task);
        }
        let delete_columns = equality_ids
            .unwrap_or_default()
            .into_iter()
            .map(|id| match schema.name_by_field_id(id) {
                Some(name) => Ok::<std::string::String, ConnectorError>(name.to_owned()),
                None => bail!("Delete field id {} not found in schema", id),
            })
            .collect::<ConnectorResult<Vec<_>>>()?;
        Ok(IcebergTaskParameters {
            data_tasks: IcebergFileScanTask::Data(data_file_scan_tasks),
            equality_delete_tasks: IcebergFileScanTask::EqualityDelete(
                equality_delete_file_scan_tasks,
            ),
            equality_delete_columns: delete_columns,
            position_delete_tasks: IcebergFileScanTask::PositionDelete(
                position_delete_file_scan_tasks,
            ),
            count: count_sum,
            snapshot_id: Some(snapshot_id),
            name_to_field_id,
        })
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
    let reader = table.reader_builder().with_batch_size(chunk_size).build();
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
        }
    }

    #[test]
    fn test_split_n_vecs_basic() {
        let file_scan_tasks = (1..=12)
            .map(|i| create_file_scan_task(i + 100, i))
            .collect::<Vec<_>>(); // Ensure the correct function is called

        let groups = IcebergFileScanTask::split_n_vecs(file_scan_tasks, 3);

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
        let groups = IcebergFileScanTask::split_n_vecs(file_scan_tasks, 3);
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|group| group.is_empty()));
    }

    #[test]
    fn test_split_n_vecs_single_task() {
        let file_scan_tasks = vec![create_file_scan_task(100, 1)];
        let groups = IcebergFileScanTask::split_n_vecs(file_scan_tasks, 3);
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

        let groups = IcebergFileScanTask::split_n_vecs(file_scan_tasks, 2);
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

        let groups = IcebergFileScanTask::split_n_vecs(file_scan_tasks.clone(), 4)
            .iter()
            .map(|g| {
                g.iter()
                    .map(|task| task.data_file_path.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for _ in 0..10000 {
            let groups_2 = IcebergFileScanTask::split_n_vecs(file_scan_tasks.clone(), 4)
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
