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
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use iceberg::Catalog;
use iceberg::expr::Predicate as IcebergPredicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::DataContentType;
use iceberg::table::Table;
use itertools::Itertools;
pub use parquet_file_handler::*;
use phf::{Set, phf_set};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_array_iceberg::Array;
use risingwave_common::array::{ArrayImpl, DataChunk, I64Array, Utf8Array};
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
    Schema,
};
use risingwave_common::types::{Datum, DatumRef, JsonbVal, ToDatumRef, ToOwnedDatum};
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

pub struct IcebergFileScanTaskBuilder {
    iceberg_scan_type: IcebergScanType,
}

impl IcebergFileScanTaskBuilder {
    pub fn new(iceberg_scan_type: IcebergScanType) -> Self {
        Self { iceberg_scan_type }
    }

    pub async fn build(
        self,
        file_scan_stream: FileScanTaskStream,
    ) -> ConnectorResult<IcebergFileScanTask> {
        async fn consume_stream<F>(mut stream: FileScanTaskStream, mut f: F) -> ConnectorResult<()>
        where
            F: FnMut(FileScanTask) -> ConnectorResult<()>,
        {
            #[for_await]
            for task in &mut stream {
                let task = task.map_err(|e| anyhow!(e))?;
                f(task)?;
            }
            Ok(())
        }

        match self.iceberg_scan_type {
            IcebergScanType::DataScan => {
                let mut file_scan_tasks = vec![];
                consume_stream(file_scan_stream, |task| {
                    if matches!(task.data_file_content, DataContentType::Data) {
                        file_scan_tasks.push(task);
                    }
                    Ok(())
                })
                .await?;
                Ok(IcebergFileScanTask::Data(file_scan_tasks))
            }
            IcebergScanType::EqualityDeleteScan => {
                let mut equality_delete_set = HashSet::new();
                let mut equality_delete_files = vec![];
                consume_stream(file_scan_stream, |task| {
                    for del_task in task.deletes {
                        if matches!(del_task.data_file_content, DataContentType::EqualityDeletes)
                            && !equality_delete_set.contains(del_task.data_file_path())
                        {
                            equality_delete_set.insert(del_task.data_file_path.clone());
                            equality_delete_files.push(del_task.as_ref().clone());
                        }
                    }
                    Ok(())
                })
                .await?;
                Ok(IcebergFileScanTask::EqualityDelete(equality_delete_files))
            }
            IcebergScanType::PositionDeleteScan => {
                let mut position_delete_set = HashSet::new();
                let mut position_delete_files = vec![];
                consume_stream(file_scan_stream, |task| {
                    for del_task in task.deletes {
                        if matches!(del_task.data_file_content, DataContentType::PositionDeletes)
                            && !position_delete_set.contains(del_task.data_file_path())
                        {
                            position_delete_set.insert(del_task.data_file_path.clone());
                            position_delete_files.push(del_task.as_ref().clone());
                        }
                    }
                    Ok(())
                })
                .await?;
                Ok(IcebergFileScanTask::PositionDelete(position_delete_files))
            }
            IcebergScanType::CountStar => {
                let mut count = 0u64;
                consume_stream(file_scan_stream, |task| {
                    if !matches!(task.data_file_content, DataContentType::Data) || !task.deletes.is_empty() {
                        bail!("Unexpected file type: {:?} in CountStar builder or task's deletes are not empty", task.data_file_content)
                    }
                    count += task.record_count.expect("must have");
                    Ok(())
                }).await?;
                Ok(IcebergFileScanTask::CountStar(count))
            }
            _ => {
                bail!("Unspecified iceberg scan type")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IcebergFileScanTask {
    Data(Vec<FileScanTask>),
    EqualityDelete(Vec<FileScanTask>),
    PositionDelete(Vec<FileScanTask>),
    CountStar(u64),
}

impl IcebergFileScanTask {
    pub fn new_count_star(count_sum: u64) -> Self {
        IcebergFileScanTask::CountStar(count_sum)
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

impl IcebergSplit {
    pub fn empty(iceberg_scan_type: IcebergScanType) -> Self {
        let task = match iceberg_scan_type {
            IcebergScanType::DataScan => IcebergFileScanTask::Data(vec![]),
            IcebergScanType::EqualityDeleteScan => IcebergFileScanTask::EqualityDelete(vec![]),
            IcebergScanType::PositionDeleteScan => IcebergFileScanTask::PositionDelete(vec![]),
            IcebergScanType::CountStar => IcebergFileScanTask::CountStar(0),
            _ => panic!("Unspecified iceberg scan type"),
        };
        IcebergSplit {
            split_id: 0,
            snapshot_id: 0,
            task,
        }
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

    pub async fn list_splits_batch(
        &self,
        schema: Schema,
        snapshot_id: Option<i64>,
        batch_parallelism: usize,
        iceberg_scan_type: IcebergScanType,
        predicate: IcebergPredicate,
    ) -> ConnectorResult<Vec<IcebergSplit>> {
        if batch_parallelism == 0 {
            bail!("Batch parallelism is 0. Cannot split the iceberg files.");
        }
        let table = self.config.load_table().await?;
        if snapshot_id.is_none() {
            // If there is no snapshot, we will return a mock `IcebergSplit` with empty files.
            return Ok(vec![IcebergSplit::empty(iceberg_scan_type)]);
        }
        self.list_splits_batch_scan(
            &table,
            snapshot_id.unwrap(),
            schema,
            batch_parallelism,
            iceberg_scan_type,
            predicate,
        )
        .await
    }

    async fn list_splits_batch_scan(
        &self,
        table: &Table,
        snapshot_id: i64,
        schema: Schema,
        batch_parallelism: usize,
        iceberg_scan_type: IcebergScanType,
        predicate: IcebergPredicate,
    ) -> ConnectorResult<Vec<IcebergSplit>> {
        let schema_names = schema.names();
        let require_names = schema_names
            .iter()
            .filter(|name| {
                name.ne(&ICEBERG_SEQUENCE_NUM_COLUMN_NAME)
                    && name.ne(&ICEBERG_FILE_PATH_COLUMN_NAME)
                    && name.ne(&ICEBERG_FILE_POS_COLUMN_NAME)
            })
            .cloned()
            .collect_vec();

        let table_schema = table.metadata().current_schema();
        tracing::debug!("iceberg_table_schema: {:?}", table_schema);
        let scan = table
            .scan()
            .with_filter(predicate)
            .snapshot_id(snapshot_id)
            .with_delete_file_processing_enabled(true)
            .select(require_names)
            .build()
            .map_err(|e| anyhow!(e))?;

        let file_scan_stream = scan.plan_files().await.map_err(|e| anyhow!(e))?;

        let task_builder = IcebergFileScanTaskBuilder::new(iceberg_scan_type);
        let file_scan_task = task_builder.build(file_scan_stream).await?;
        let split_tasks = file_scan_task.split(batch_parallelism)?;

        // evenly split the files into splits based on the parallelism.
        let splits = split_tasks
            .into_iter()
            .enumerate()
            .map(|(index, task)| IcebergSplit {
                split_id: index as i64,
                snapshot_id,
                task,
            })
            .filter(|split| !split.task.is_empty())
            .collect_vec();
        if splits.is_empty() {
            return Ok(vec![IcebergSplit::empty(iceberg_scan_type)]);
        }
        Ok(splits)
    }

    /// List all files in the snapshot to check if there are deletes.
    pub async fn all_delete_parameters(
        table: &Table,
        snapshot_id: i64,
    ) -> ConnectorResult<(Vec<String>, bool)> {
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_delete_file_processing_enabled(true)
            .build()
            .map_err(|e| anyhow!(e))?;
        let file_scan_stream = scan.plan_files().await.map_err(|e| anyhow!(e))?;
        let schema = scan.snapshot().schema(table.metadata())?;
        let mut equality_ids = vec![];
        let mut have_position_delete = false;
        #[for_await]
        for task in file_scan_stream {
            let task: FileScanTask = task.map_err(|e| anyhow!(e))?;
            for delete_file in task.deletes {
                match delete_file.data_file_content {
                    iceberg::spec::DataContentType::Data => {}
                    iceberg::spec::DataContentType::EqualityDeletes => {
                        if equality_ids.is_empty() {
                            equality_ids = delete_file.equality_ids.clone();
                        } else if equality_ids != delete_file.equality_ids {
                            bail!("The schema of iceberg equality delete file must be consistent");
                        }
                    }
                    iceberg::spec::DataContentType::PositionDeletes => {
                        have_position_delete = true;
                    }
                }
            }
        }
        let delete_columns = equality_ids
            .into_iter()
            .map(|id| match schema.name_by_field_id(id) {
                Some(name) => Ok::<std::string::String, ConnectorError>(name.to_owned()),
                None => bail!("Delete field id {} not found in schema", id),
            })
            .collect::<ConnectorResult<Vec<_>>>()?;

        Ok((delete_columns, have_position_delete))
    }

    pub async fn get_delete_parameters(
        &self,
        time_travel_info: Option<IcebergTimeTravelInfo>,
    ) -> ConnectorResult<IcebergDeleteParameters> {
        let table = self.config.load_table().await?;
        let snapshot_id = Self::get_snapshot_id(&table, time_travel_info)?;
        match snapshot_id {
            Some(snapshot_id) => {
                let (delete_columns, have_position_delete) =
                    Self::all_delete_parameters(&table, snapshot_id).await?;
                Ok(IcebergDeleteParameters {
                    equality_delete_columns: delete_columns,
                    has_position_delete: have_position_delete,
                    snapshot_id: Some(snapshot_id),
                })
            }
            None => Ok(IcebergDeleteParameters {
                equality_delete_columns: vec![],
                has_position_delete: false,
                snapshot_id: None,
            }),
        }
    }
}

pub struct IcebergScanOpts {
    pub chunk_size: usize,
    pub need_seq_num: bool,
    pub need_file_path_and_pos: bool,
    pub handle_delete_files: bool,
}

type EqualityDeleteEntries = Vec<(Vec<String>, HashSet<Vec<Datum>>)>;

/// Scan a data file and optionally apply delete files (both position delete and equality delete).
/// This implementation follows the iceberg-rust `process_file_scan_task` logic for proper delete handling.
#[try_stream(ok = DataChunk, error = ConnectorError)]
pub async fn scan_task_to_chunk_with_deletes(
    table: Table,
    data_file_scan_task: FileScanTask,
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

    // Step 1: Load and process delete files
    // We build both position deletes (sorted set of row positions) and equality deletes (hash set of row keys)

    // Build position delete vector - using BTreeSet for sorted storage (similar to DeleteVector in iceberg-rust)
    let position_delete_set: BTreeSet<i64> = if handle_delete_files {
        let mut deletes = BTreeSet::new();

        // Filter position delete tasks for this specific data file
        let position_delete_tasks: Vec<_> = data_file_scan_task
            .deletes
            .iter()
            .filter(|delete| delete.data_file_content == DataContentType::PositionDeletes)
            .cloned()
            .collect();

        tracing::debug!(
            "Processing position deletes for data file: {}, found {} position delete tasks",
            data_file_path,
            position_delete_tasks.len()
        );

        for delete_task in position_delete_tasks {
            let delete_task_file_path = delete_task.data_file_path.clone();

            let delete_reader = table.reader_builder().with_batch_size(chunk_size).build();
            // Clone the FileScanTask (not Arc) to create a proper stream
            let task_clone: FileScanTask = (*delete_task).clone();
            let delete_stream = tokio_stream::once(Ok(task_clone));
            let mut delete_record_stream = delete_reader.read(Box::pin(delete_stream)).await?;

            while let Some(record_batch) = delete_record_stream.next().await {
                let record_batch = record_batch?;

                // Position delete files have schema: file_path (string), pos (long)
                if let Some(file_path_col) = record_batch.column_by_name("file_path")
                    && let Some(pos_col) = record_batch.column_by_name("pos")
                {
                    let file_paths = file_path_col
                            .as_any()
                            .downcast_ref::<risingwave_common::array::arrow::arrow_array_iceberg::StringArray>()
                            .with_context(|| "file_path column is not StringArray")?;
                    let positions = pos_col
                            .as_any()
                            .downcast_ref::<risingwave_common::array::arrow::arrow_array_iceberg::Int64Array>()
                            .with_context(|| "pos column is not Int64Array")?;

                    // Only include positions that match the current data file
                    for idx in 0..record_batch.num_rows() {
                        if !file_paths.is_null(idx) && !positions.is_null(idx) {
                            let file_path = file_paths.value(idx);
                            let pos = positions.value(idx);

                            if file_path == data_file_path {
                                deletes.insert(pos);
                            }
                        } else {
                            tracing::warn!(
                                "Position delete file {} at line {}: file_path or pos is null",
                                delete_task_file_path,
                                idx
                            );
                        }
                    }
                }
            }
        }

        tracing::debug!(
            "Built position delete set for data file {}: {:?}",
            data_file_path,
            deletes
        );

        deletes
    } else {
        BTreeSet::new()
    };

    // Build equality delete predicates
    let equality_deletes: Option<EqualityDeleteEntries> = if handle_delete_files {
        let equality_delete_tasks: Vec<_> = data_file_scan_task
            .deletes
            .iter()
            .filter(|delete| delete.data_file_content == DataContentType::EqualityDeletes)
            .cloned()
            .collect();

        if !equality_delete_tasks.is_empty() {
            let mut delete_key_map: HashMap<Vec<String>, HashSet<Vec<Datum>>> = HashMap::new();

            for delete_task in equality_delete_tasks {
                let equality_ids = delete_task.equality_ids.clone();

                if equality_ids.is_empty() {
                    continue;
                }

                let delete_schema = delete_task.schema();
                let delete_name_vec = equality_ids
                    .iter()
                    .filter_map(|id| delete_schema.name_by_field_id(*id))
                    .map(|s| s.to_owned())
                    .collect_vec();

                if delete_name_vec.len() != equality_ids.len() {
                    tracing::warn!(
                        "Skip equality delete task due to missing column mappings: expected {} names, got {}",
                        equality_ids.len(),
                        delete_name_vec.len()
                    );
                    continue;
                }

                let delete_reader = table.reader_builder().with_batch_size(chunk_size).build();
                // Clone the FileScanTask (not Arc) to create a proper stream
                let task_clone: FileScanTask = delete_task.as_ref().clone();
                let delete_stream = tokio_stream::once(Ok(task_clone));
                let mut delete_record_stream = delete_reader.read(Box::pin(delete_stream)).await?;

                let mut task_delete_key_set: HashSet<Vec<Datum>> = HashSet::new();

                while let Some(record_batch) = delete_record_stream.next().await {
                    let record_batch = record_batch?;

                    let delete_chunk =
                        IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;

                    let field_indices = delete_name_vec
                        .iter()
                        .map(|field_name| {
                            record_batch
                                .schema()
                                .column_with_name(field_name)
                                .map(|(idx, _)| idx)
                                .unwrap()
                        })
                        .collect_vec();

                    // Build delete keys from equality columns
                    for row_idx in 0..delete_chunk.capacity() {
                        let mut key = Vec::with_capacity(field_indices.len());
                        for &col_idx in &field_indices {
                            let col = delete_chunk.column_at(col_idx);
                            key.push(col.value_at(row_idx).to_owned_datum());
                        }
                        task_delete_key_set.insert(key);
                    }
                }

                if !task_delete_key_set.is_empty() {
                    delete_key_map
                        .entry(delete_name_vec.clone())
                        .or_default()
                        .extend(task_delete_key_set);
                }
            }

            if delete_key_map.is_empty() {
                None
            } else {
                Some(delete_key_map.into_iter().collect())
            }
        } else {
            None
        }
    } else {
        None
    };

    // Step 2: Read the data file
    let reader = table.reader_builder().with_batch_size(chunk_size).build();
    let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task));

    let mut record_batch_stream = reader.read(Box::pin(file_scan_stream)).await?.enumerate();

    // Step 3: Process each record batch and apply deletes
    while let Some((batch_index, record_batch)) = record_batch_stream.next().await {
        let record_batch = record_batch?;
        let batch_start_pos = (batch_index * chunk_size) as i64;
        let batch_num_rows = record_batch.num_rows();

        let mut chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;

        // Apply position deletes using efficient range-based filtering
        // Build visibility bitmap based on position deletes
        let mut visibility = vec![true; batch_num_rows];

        if !position_delete_set.is_empty() {
            let batch_end_pos = batch_start_pos + batch_num_rows as i64;

            tracing::debug!(
                "Applying position deletes to batch {}: range [{}, {}), total delete set size: {}",
                batch_index,
                batch_start_pos,
                batch_end_pos,
                position_delete_set.len()
            );

            let mut position_deleted_count = 0;

            // Use BTreeSet's range query for efficient filtering
            // Only check positions that fall within this batch's range
            for &deleted_pos in position_delete_set.range(batch_start_pos..batch_end_pos) {
                let local_idx = (deleted_pos - batch_start_pos) as usize;
                if local_idx < batch_num_rows {
                    visibility[local_idx] = false;
                    position_deleted_count += 1;
                }
            }

            tracing::debug!(
                "Position delete results for batch {}: deleted {} rows",
                batch_index,
                position_deleted_count
            );
        }

        // Apply equality deletes
        if let Some(ref equality_delete_entries) = equality_deletes {
            let (columns, _) = chunk.into_parts();

            let delete_entries_info: Vec<(Vec<usize>, HashSet<Vec<DatumRef<'_>>>)> =
                equality_delete_entries
                    .iter()
                    .map(|(delete_name_vec, delete_key_set)| {
                        let indices = delete_name_vec
                            .iter()
                            .map(|field_name| {
                                record_batch
                                    .schema()
                                    .column_with_name(field_name)
                                    .map(|(idx, _)| idx)
                                    .unwrap()
                            })
                            .collect_vec();
                        let delete_key_ref_set: HashSet<Vec<DatumRef<'_>>> = delete_key_set
                            .iter()
                            .map(|datum_vec| {
                                datum_vec.iter().map(|datum| datum.to_datum_ref()).collect()
                            })
                            .collect();
                        (indices, delete_key_ref_set)
                    })
                    .collect::<Vec<_>>();

            let mut deleted_count = 0;

            // Check each row against the delete sets built per equality delete task
            for (row_idx, item) in visibility.iter_mut().enumerate().take(batch_num_rows) {
                if !*item {
                    continue;
                }

                for (field_indices, delete_key_set) in &delete_entries_info {
                    let mut row_key = Vec::with_capacity(field_indices.len());
                    for &col_idx in field_indices {
                        let datum = columns[col_idx].value_at(row_idx);
                        row_key.push(datum);
                    }

                    if delete_key_set.contains(&row_key) {
                        *item = false;
                        deleted_count += 1;
                        break;
                    }
                }
            }

            tracing::debug!(
                "Equality delete results for batch {}: deleted {} rows",
                batch_index,
                deleted_count
            );

            let columns: Vec<_> = columns.into_iter().collect();
            chunk = DataChunk::from_parts(columns.into(), Bitmap::from_bool_slice(&visibility));
        } else {
            // Only position deletes to apply
            let (columns, _) = chunk.into_parts();
            let columns: Vec<_> = columns.into_iter().collect();
            chunk = DataChunk::from_parts(columns.into(), Bitmap::from_bool_slice(&visibility));
        }

        // Step 4: Add metadata columns if requested
        if need_seq_num {
            let (mut columns, visibility) = chunk.into_parts();
            columns.push(Arc::new(ArrayImpl::Int64(I64Array::from_iter(
                vec![data_sequence_number; visibility.len()],
            ))));
            chunk = DataChunk::from_parts(columns.into(), visibility);
        }

        if need_file_path_and_pos {
            let (mut columns, visibility) = chunk.into_parts();
            columns.push(Arc::new(ArrayImpl::Utf8(Utf8Array::from_iter(
                vec![data_file_path.as_str(); visibility.len()],
            ))));

            // Generate position values for each row in the batch
            let positions: Vec<i64> =
                (batch_start_pos..(batch_start_pos + visibility.len() as i64)).collect();
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
            equality_ids: vec![],
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
