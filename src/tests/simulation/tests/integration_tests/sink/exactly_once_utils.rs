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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::iter::once;
use std::mem::take;
use std::num::NonZero;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicUsize};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use futures::future::pending;
use futures::stream::{BoxStream, empty, select_all};
use futures::{FutureExt, StreamExt, stream};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::{Rng, rng as thread_rng};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarImpl, Serial};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::coordinate::CoordinatedLogSinker;
use risingwave_connector::sink::test_sink::{
    TestSinkRegistryGuard, register_build_coordinated_sink, register_build_sink,
};
use risingwave_connector::sink::writer::SinkWriter;
use risingwave_connector::sink::{SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkError};
use risingwave_connector::source::test_source::{
    BoxSource, TestSourceRegistryGuard, TestSourceSplit, register_test_source,
};
use risingwave_meta_model::exactly_once_iceberg_sink::{self, Column, Entity, Model};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::{Metadata, SerializedMetadata};
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, Order, PaginatorTrait, QueryFilter, QueryOrder,
    Set,
};
use tokio::task::yield_now;
use tokio::time::sleep;

use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

/// `TestSinkStoreInner` is designed to mock the Iceberg behavior with exactly-once
/// semantics. It utilizes a `BTreeMap<snapshot_id, HashMap<id, name>>` to simulate
/// an external Iceberg. Each commit in Iceberg is accompanied by a unique snapshot ID,
/// which serves as the key. By checking whether a specific `snapshot_id` exists in
/// the `snapshot_id_to_data` BTreeMap, we can replicate the real-world scenario of
/// verifying if data from a particular commit exists in Iceberg.
///
/// Note: Iceberg operates under the principle of transactional commits; therefore,
/// there is no scenario where part of a batch succeeds while another part fails.
pub struct TestSinkStoreInner {
    id_name: HashMap<i32, Vec<String>>,
    // snapshot_id -> id + name map
    snapshot_id_to_data: BTreeMap<i64, HashMap<i32, Vec<String>>>,
    epochs: Vec<u64>,

    checkpoint_count: usize,
    err_count: usize,
}

#[derive(Clone)]
pub struct TestSinkStore {
    inner: Arc<Mutex<TestSinkStoreInner>>,
}

impl TestSinkStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestSinkStoreInner {
                id_name: HashMap::new(),
                snapshot_id_to_data: BTreeMap::new(),
                epochs: Vec::new(),
                checkpoint_count: 0,
                err_count: 0,
            })),
        }
    }

    pub fn insert_many(&self, snapshot_id: i64, pairs: impl Iterator<Item = (i32, Vec<String>)>) {
        let mut inner = self.inner();
        if inner.snapshot_id_to_data.contains_key(&snapshot_id) {
            panic!("snapshot_id {} already exists!", snapshot_id);
        }
        let entry = inner.snapshot_id_to_data.entry(snapshot_id).or_default();

        for (id, names) in pairs {
            entry.entry(id).or_default().extend(names);
        }
    }

    pub fn insert(&self, id: i32, name: String) {
        self.inner().id_name.entry(id).or_default().push(name);
    }

    pub fn begin_epoch(&self, epoch: u64) {
        self.inner().epochs.push(epoch)
    }

    fn inner(&self) -> MutexGuard<'_, TestSinkStoreInner> {
        self.inner.lock().unwrap()
    }

    pub fn count_total_keys(&self) -> usize {
        let mut inner = self.inner();
        let mut total_keys = 0;

        for inner_map in inner.snapshot_id_to_data.values() {
            total_keys += inner_map.len();
        }

        total_keys
    }

    pub fn check_simple_result(&self, id_list: &[i32]) -> anyhow::Result<()> {
        println!("total id_list count is {}", id_list.len());
        let mut store_id_name_list = HashMap::new();
        let mut inner = self.inner();
        let mut seen_keys = HashSet::new();
        for inner_map in inner.snapshot_id_to_data.values() {
            for (key, value) in inner_map {
                if seen_keys.contains(key) {
                    panic!("Duplicate key found: {}", key);
                } else {
                    seen_keys.insert(*key);
                    store_id_name_list
                        .entry(*key)
                        .or_insert_with(Vec::new)
                        .extend(value.clone());
                }
            }
        }
        println!("store_id_name_list.len() = {:?}", store_id_name_list.len());
        assert_eq!(store_id_name_list.len(), id_list.len());
        for id in id_list {
            let names = store_id_name_list.get(id).unwrap();
            assert!(!names.is_empty());
            for name in names {
                assert_eq!(name, &simple_name_of_id(*id));
            }
        }
        Ok(())
    }

    pub fn id_count(&self) -> usize {
        self.inner()
            .snapshot_id_to_data
            .values()
            .map(|map| map.len())
            .sum()
    }

    pub fn inc_checkpoint(&self) {
        self.inner().checkpoint_count += 1;
    }

    pub fn checkpoint_count(&self) -> usize {
        self.inner().checkpoint_count
    }

    pub fn inc_err(&self) {
        self.inner().err_count += 1;
    }

    pub fn err_count(&self) -> usize {
        self.inner().err_count
    }

    pub async fn wait_for_count_and_check_duplicate(&self, count: usize) -> anyhow::Result<()> {
        let mut prev_count = 0;
        let mut has_printed = false;
        let mut consecutive_matches = 0;
        loop {
            sleep(Duration::from_secs(1)).await;
            let curr_count = self.id_count();
            if curr_count > count {
                panic!(
                    "Not exactly once, duplicated! current count {:?} is greater than expect count {:?}",
                    curr_count, count
                );
            }

            if curr_count == count {
                consecutive_matches += 1;
                if consecutive_matches >= 5 {
                    break;
                }
            } else {
                consecutive_matches = 0;
            }
            if curr_count == prev_count {
                if !has_printed {
                    println!("not making progress: curr {}", curr_count);
                    has_printed = true;
                }
                sleep(Duration::from_secs(3)).await;
            } else {
                prev_count = curr_count;
                has_printed = false;
            }
        }
        Ok(())
    }

    pub async fn wait_for_err(&self, count: usize) -> anyhow::Result<()> {
        loop {
            sleep(Duration::from_secs(1)).await;
            if self.err_count() >= count {
                break;
            }
        }
        Ok(())
    }
}

pub struct TestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
    err_rate: Arc<AtomicU32>,
}

#[async_trait]
impl SinkWriter for TestWriter {
    async fn begin_epoch(&mut self, epoch: u64) -> risingwave_connector::sink::Result<()> {
        self.store.begin_epoch(epoch);
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> risingwave_connector::sink::Result<()> {
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            assert_eq!(row.len(), 2);
            let id = row.datum_at(0).unwrap().into_int32();
            let name = row.datum_at(1).unwrap().into_utf8().to_string();
            self.store.insert(id, name);
        }
        Ok(())
    }

    async fn barrier(
        &mut self,
        is_checkpoint: bool,
    ) -> risingwave_connector::sink::Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.store.inc_checkpoint();
            sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}

impl Drop for TestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

#[derive(Default)]
struct StagingDataStoreInner {
    next_file_id: usize,
    files: HashMap<usize, HashMap<i32, Vec<String>>>,
}

#[derive(Default, Clone)]
struct StagingDataStore {
    inner: Arc<Mutex<StagingDataStoreInner>>,
}

impl StagingDataStore {
    fn add(&self, id_names: HashMap<i32, Vec<String>>) -> SinkMetadata {
        let mut inner = self.inner.lock().unwrap();
        let file_id = inner.next_file_id;
        inner.next_file_id += 1;
        inner.files.insert(file_id, id_names);
        SinkMetadata {
            metadata: Some(Metadata::Serialized(SerializedMetadata {
                metadata: file_id.to_le_bytes().into(),
            })),
        }
    }

    fn get(&self, file_id: usize) -> HashMap<i32, Vec<String>> {
        self.inner.lock().unwrap().files[&file_id].clone()
    }
}

pub struct CoordinatedTestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
    err_rate: Arc<AtomicU32>,
    staging: HashMap<i32, Vec<String>>,
    staging_store: StagingDataStore,
}

impl Drop for CoordinatedTestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

#[async_trait]
impl SinkWriter for CoordinatedTestWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn begin_epoch(&mut self, epoch: u64) -> risingwave_connector::sink::Result<()> {
        self.store.begin_epoch(epoch);
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> risingwave_connector::sink::Result<()> {
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            assert_eq!(row.len(), 2);
            let id = row.datum_at(0).unwrap().into_int32();
            let name = row.datum_at(1).unwrap().into_utf8().to_string();
            self.staging.entry(id).or_default().push(name);
        }
        Ok(())
    }

    async fn barrier(
        &mut self,
        is_checkpoint: bool,
    ) -> risingwave_connector::sink::Result<Self::CommitMetadata> {
        let metadata = if is_checkpoint {
            self.store.inc_checkpoint();
            sleep(Duration::from_millis(100)).await;
            Some(self.staging_store.add(take(&mut self.staging)))
        } else {
            None
        };
        Ok(metadata)
    }
}

/// `SimulationTestIcebergCommitter` mocks the behavior of the Iceberg committer
/// with exactly-once semantics. During the commit process, it first pre-commits
/// metadata to the meta store. After a successful commit, it deletes the previously
/// persisted metadata. In the recovery phase, it determines how to perform exactly-once
/// data recovery based on the system table information and `snapshot_id` from the
/// meta store. While the actual sinking process mocks the Iceberg behavior, all
/// other logic remains consistent with the real Iceberg committer.
pub struct SimulationTestIcebergCommitter {
    store: TestSinkStore,
    err_rate: Arc<AtomicU32>,
    last_commit_epoch: u64,
    staging_store: StagingDataStore,
    db: DatabaseConnection,
    committed_epoch_subscriber: Option<SinkCommittedEpochSubscriber>,
    sink_id: SinkId,
}

#[async_trait]
impl SinkCommitCoordinator for SimulationTestIcebergCommitter {
    async fn init(
        &mut self,
        subscriber: SinkCommittedEpochSubscriber,
    ) -> risingwave_connector::sink::Result<Option<u64>> {
        self.committed_epoch_subscriber = Some(subscriber);
        if self
            .iceberg_sink_has_pre_commit_metadata(&self.db, self.sink_id.sink_id())
            .await?
        {
            println!(
                "Sink id = {}: System table not empty! Recovery occurs!",
                self.sink_id.sink_id()
            );
            let ordered_metadata_list_by_end_epoch = self
                .get_pre_commit_info_by_sink_id(&self.db, self.sink_id.sink_id())
                .await?;
            let mut last_recommit_epoch = 0;
            for (end_epoch, sealized_bytes, snapshot_id, committed) in
                ordered_metadata_list_by_end_epoch
            {
                let write_results_bytes: Vec<Vec<u8>> = deserialize_metadata(sealized_bytes);
                let mut sink_metadatas: Vec<SinkMetadata> = vec![];

                for each in write_results_bytes {
                    sink_metadatas.push(SinkMetadata {
                        metadata: Some(Metadata::Serialized(SerializedMetadata { metadata: each })),
                    });
                }

                if thread_rng().random_ratio(self.err_rate.load(Relaxed), u32::MAX) {
                    println!("Time period 0 -- Error occur during recovery.");
                    self.store.inc_err();
                    return Err(SinkError::Internal(anyhow::anyhow!("fail to recovery")));
                }
                match (
                    committed,
                    self.is_snapshot_id_in_iceberg(snapshot_id).await?,
                ) {
                    (true, _) => {
                        println!(
                            "Sink id = {}: all data in log store has been written into external sink, do nothing when recovery.",
                            self.sink_id.sink_id()
                        );
                    }
                    (false, true) => {
                        // skip
                        println!(
                            "Sink id = {}: all pre-commit files have been successfully committed into iceberg and do not need to be committed again, mark it as committed.",
                            self.sink_id.sink_id()
                        );
                        self.mark_row_is_committed_by_sink_id_and_end_epoch(
                            &self.db,
                            self.sink_id.sink_id(),
                            end_epoch,
                        )
                        .await?;
                    }
                    (false, false) => {
                        println!(
                            "Sink id = {}: there are files that were not successfully committed; re-commit these files.",
                            self.sink_id.sink_id()
                        );
                        self.re_commit(end_epoch, sink_metadatas, snapshot_id)
                            .await?;
                    }
                }

                last_recommit_epoch = end_epoch;
            }
            println!(
                "Sink id = {}: Recovery finished! Rewind log store from end_epoch = {}",
                self.sink_id.sink_id(),
                last_recommit_epoch
            );
            return Ok(Some(last_recommit_epoch));
        } else {
            println!(
                "Sink id = {}: init iceberg coodinator, and system table is empty.",
                self.sink_id.sink_id()
            );
            return Ok(None);
        }
    }

    /// After collecting the metadata from each sink writer, a coordinator will call `commit` with
    /// the set of metadata. The metadata is serialized into bytes, because the metadata is expected
    /// to be passed between different gRPC node, so in this general trait, the metadata is
    /// serialized bytes.
    async fn commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
    ) -> risingwave_connector::sink::Result<()> {
        // wait epoch
        assert!(self.committed_epoch_subscriber.is_some());
        match self.committed_epoch_subscriber.clone() {
            Some(committed_epoch_subscriber) => {
                // Get the latest committed_epoch and the receiver
                let (committed_epoch, mut rw_futures_utilrx) =
                    committed_epoch_subscriber(self.sink_id).await?;
                // The exactly once commit process needs to start after the data corresponding to the current epoch is persisted in the log store.
                if committed_epoch >= epoch {
                    self.commit_inner(epoch, metadata, None).await?;
                } else {
                    println!(
                        "Waiting for the committed epoch to rise. Current: {}, Waiting for: {}",
                        committed_epoch,
                        epoch
                    );
                    while let Some(next_committed_epoch) = rw_futures_utilrx.recv().await {
                        println!("Received next committed epoch: {}", next_committed_epoch);
                        // If next_epoch meets the condition, execute commit immediately
                        if next_committed_epoch >= epoch {
                            self.commit_inner(epoch, metadata, None).await?;
                            break;
                        }
                    }
                }
            }
            None => unreachable!(
                "Exactly once sink must wait epoch before committing, committed_epoch_subscriber is not initialized."
            ),
        }
        Ok(())
    }
}

impl SimulationTestIcebergCommitter {
    async fn commit_inner(
        &mut self,
        epoch: u64,
        metadatas: Vec<SinkMetadata>,
        snapshot_id: Option<i64>,
    ) -> risingwave_connector::sink::Result<()> {
        let is_first_commit = snapshot_id.is_none();
        if !is_first_commit {
            println!("Doing iceberg re commit.");
        }
        self.last_commit_epoch = epoch;

        let snapshot_id = match snapshot_id {
            Some(previous_snapshot_id) => previous_snapshot_id,
            None => generate_unique_snapshot_id(epoch),
        };

        if thread_rng().random_ratio(self.err_rate.load(Relaxed), u32::MAX) {
            println!("Time period 1 -- Error occur before pre commit to meta store.");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to pre commit to meta store"
            )));
        }
        if is_first_commit {
            // persist pre commit metadata and snapshot id in system table.
            let mut pre_commit_metadata_bytes = Vec::new();
            for metadata in metadatas.clone() {
                let Metadata::Serialized(serialized) = metadata.metadata.unwrap();

                pre_commit_metadata_bytes.push(serialized.metadata);
            }

            let pre_commit_metadata_bytes: Vec<u8> = serialize_metadata(pre_commit_metadata_bytes);

            self.persist_pre_commit_metadata(
                self.db.clone(),
                self.last_commit_epoch,
                epoch,
                pre_commit_metadata_bytes,
                snapshot_id,
            )
            .await?;
        }
        if thread_rng().random_ratio(self.err_rate.load(Relaxed), u32::MAX) {
            println!(
                "Time period 2 -- Error occur before commit to external sink and after pre commit."
            );
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to commit to external sink."
            )));
        }
        let file_ids = metadatas.into_iter().map(|metadata| {
            let Metadata::Serialized(serialized) = metadata.metadata.unwrap();
            usize::from_le_bytes((serialized.metadata.as_slice()).try_into().unwrap())
        });
        self.store.insert_many(
            snapshot_id,
            file_ids
                .into_iter()
                .map(|file_id| self.staging_store.get(file_id))
                .flatten(),
        );
        println!("Succeeded to commit to mock iceberg table in epoch {epoch}.");

        if thread_rng().random_ratio(self.err_rate.load(Relaxed), u32::MAX) {
            println!(
                "Time period 3 -- Error occur after commit to external sink and before mark row deleted."
            );
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to mark row deleted."
            )));
        }

        self.mark_row_is_committed_by_sink_id_and_end_epoch(
            &self.db,
            self.sink_id.sink_id(),
            epoch,
        )
        .await?;
        println!(
            "Sink id = {}: succeeded mark pre commit metadata in epoch {} to deleted.",
            self.sink_id, epoch
        );

        if thread_rng().random_ratio(self.err_rate.load(Relaxed), u32::MAX) {
            println!(
                "Time period 4 -- Error occur after mark committed before deleting previous item."
            );
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to delete previous item in meta store."
            )));
        }

        self.delete_row_by_sink_id_and_end_epoch(&self.db, self.sink_id.sink_id(), epoch)
            .await?;
        Ok(())
    }

    async fn re_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        snapshot_id: i64,
    ) -> risingwave_connector::sink::Result<()> {
        println!("Starting mock iceberg re commit in epoch {}.", epoch);
        self.commit_inner(epoch, metadata, Some(snapshot_id))
            .await?;
        Ok(())
    }
}

// mock iceberg sink handle system table
impl SimulationTestIcebergCommitter {
    async fn persist_pre_commit_metadata(
        &self,
        db: DatabaseConnection,
        start_epoch: u64,
        end_epoch: u64,
        pre_commit_metadata: Vec<u8>,
        snapshot_id: i64,
    ) -> Result<()> {
        let m = exactly_once_iceberg_sink::ActiveModel {
            sink_id: Set(self.sink_id.sink_id() as i32),
            end_epoch: Set(end_epoch.try_into().unwrap()),
            start_epoch: Set(start_epoch.try_into().unwrap()),
            metadata: Set(pre_commit_metadata),
            committed: Set(false),
            snapshot_id: Set(snapshot_id),
        };
        match exactly_once_iceberg_sink::Entity::insert(m).exec(&db).await {
            Ok(_) => Ok(()),
            Err(e) => {
                println!("Error inserting into system table: {:?}", e);
                Err(e.into())
            }
        }
    }

    async fn mark_row_is_committed_by_sink_id_and_end_epoch(
        &self,
        db: &DatabaseConnection,
        sink_id: u32,
        end_epoch: u64,
    ) -> Result<()> {
        match Entity::update(exactly_once_iceberg_sink::ActiveModel {
            sink_id: Set(sink_id as i32),
            end_epoch: Set(end_epoch.try_into().unwrap()),
            committed: Set(true),
            ..Default::default()
        })
        .exec(db)
        .await
        {
            Ok(_) => {
                println!(
                    "Sink id = {}: mark written data status to committed, end_epoch = {}.",
                    sink_id, end_epoch
                );
                Ok(())
            }
            Err(e) => {
                println!(
                    "Error marking item to committed from iceberg exactly once system table: {:?}",
                    e
                );
                Err(e.into())
            }
        }
    }

    async fn delete_row_by_sink_id_and_end_epoch(
        &self,
        db: &DatabaseConnection,
        sink_id: u32,
        end_epoch: u64,
    ) -> Result<()> {
        let end_epoch_i64: i64 = end_epoch.try_into().unwrap();
        match Entity::delete_many()
            .filter(Column::SinkId.eq(sink_id))
            .filter(Column::EndEpoch.lt(end_epoch_i64))
            .exec(db)
            .await
        {
            Ok(result) => {
                let deleted_count = result.rows_affected;

                if deleted_count == 0 {
                    println!(
                        "Sink id = {}: no item deleted in iceberg exactly once system table, end_epoch < {}.",
                        sink_id, end_epoch
                    );
                } else {
                    println!(
                        "Sink id = {}: deleted item in iceberg exactly once system table, end_epoch < {}.",
                        sink_id, end_epoch
                    );
                }
                Ok(())
            }
            Err(e) => {
                println!(
                    "Sink id = {}: error deleting from iceberg exactly once system table: {:?}",
                    sink_id, e
                );
                Err(e.into())
            }
        }
    }

    async fn iceberg_sink_has_pre_commit_metadata(
        &self,
        db: &DatabaseConnection,
        sink_id: u32,
    ) -> Result<bool> {
        match exactly_once_iceberg_sink::Entity::find()
            .filter(exactly_once_iceberg_sink::Column::SinkId.eq(sink_id as i32))
            .count(db)
            .await
        {
            Ok(count) => Ok(count > 0),
            Err(e) => {
                println!(
                    "Error querying pre-commit metadata from system table: {:?}",
                    e
                );
                Err(e.into())
            }
        }
    }

    async fn get_pre_commit_info_by_sink_id(
        &self,
        db: &DatabaseConnection,
        sink_id: u32,
    ) -> Result<Vec<(u64, Vec<u8>, i64, bool)>> {
        let models: Vec<Model> = Entity::find()
            .filter(Column::SinkId.eq(sink_id as i32))
            .order_by(Column::EndEpoch, Order::Asc)
            .all(db)
            .await?;

        let mut result: Vec<(u64, Vec<u8>, i64, bool)> = Vec::new();

        for model in models {
            result.push((
                model.end_epoch.try_into().unwrap(),
                model.metadata,
                model.snapshot_id,
                model.committed,
            ));
        }

        Ok(result)
    }

    async fn is_snapshot_id_in_iceberg(&self, snapshot_id: i64) -> Result<bool> {
        Ok(self
            .store
            .inner()
            .snapshot_id_to_data
            .contains_key(&snapshot_id))
    }
}

pub fn simple_name_of_id(id: i32) -> String {
    format!("name-{}", id)
}

fn generate_unique_snapshot_id(epoch: u64) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let timestamp_nanos = now.as_nanos() as i64;
    let unique_snapshot_id = timestamp_nanos.wrapping_add(233);
    unique_snapshot_id as i64
}

pub struct SimulationTestIcebergExactlyOnceSink {
    _sink_guard: TestSinkRegistryGuard,
    pub store: TestSinkStore,
    pub parallelism_counter: Arc<AtomicUsize>,
    pub err_rate: Arc<AtomicU32>,
}

impl Drop for SimulationTestIcebergExactlyOnceSink {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

impl SimulationTestIcebergExactlyOnceSink {
    pub fn register_new_with_err_rate(err_rate: f64) -> Self {
        let parallelism_counter = Arc::new(AtomicUsize::new(0));

        let err_rate = u32::MAX as f64 * err_rate;
        let err_rate_for_writer = Arc::new(AtomicU32::new(0));
        let err_rate_for_committer = Arc::new(AtomicU32::new(err_rate as u32));

        let store = TestSinkStore::new();

        let _sink_guard = {
            let staging_store = StagingDataStore::default();
            register_build_coordinated_sink(
                {
                    let parallelism_counter = parallelism_counter.clone();
                    let err_rate = err_rate_for_committer.clone();
                    let store = store.clone();
                    let staging_store = staging_store.clone();
                    use risingwave_connector::sink::SinkWriterMetrics;
                    use risingwave_connector::sink::writer::SinkWriterExt;
                    move |param, writer_param| {
                        parallelism_counter.fetch_add(1, Relaxed);
                        let metrics = SinkWriterMetrics::new(&writer_param);
                        let writer = CoordinatedTestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                            err_rate: err_rate.clone(),
                            staging_store: staging_store.clone(),
                            staging: Default::default(),
                        };
                        async move {
                            let log_sinker = risingwave_connector::sink::boxed::boxed_log_sinker(
                                CoordinatedLogSinker::new(
                                    &writer_param,
                                    param,
                                    writer,
                                    NonZero::new(1).unwrap(),
                                )
                                .await?,
                            );
                            Ok(log_sinker)
                        }
                        .boxed()
                    }
                },
                {
                    let err_rate = err_rate_for_committer.clone();
                    let store = store.clone();
                    let staging_store = staging_store.clone();
                    move |db, sink_id| {
                        Box::new(SimulationTestIcebergCommitter {
                            err_rate: err_rate.clone(),
                            store: store.clone(),
                            last_commit_epoch: 0,
                            staging_store: staging_store.clone(),
                            db: db.clone(),
                            committed_epoch_subscriber: None,
                            sink_id: sink_id,
                        })
                    }
                },
            )
        };
        Self {
            _sink_guard,
            parallelism_counter,
            store,
            err_rate: err_rate_for_writer.clone(),
        }
    }

    pub async fn wait_initial_parallelism(&self, parallelism: usize) -> Result<()> {
        while self.parallelism_counter.load(Relaxed) < parallelism {
            yield_now().await;
        }
        assert_eq!(self.parallelism_counter.load(Relaxed), parallelism);
        Ok(())
    }
}

pub fn serialize_metadata(metadata: Vec<Vec<u8>>) -> Vec<u8> {
    serde_json::to_vec(&metadata).unwrap()
}

pub fn deserialize_metadata(bytes: Vec<u8>) -> Vec<Vec<u8>> {
    serde_json::from_slice(&bytes).unwrap()
}
