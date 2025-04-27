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
use risingwave_connector::sink::iceberg::exactly_once_util::*;
use risingwave_connector::sink::iceberg::{CommitIceberg, IcebergSinkCommitter};
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
    // err_events is used to record the history and sequence of error injections, facilitating the inspection of whether specific corner cases are triggered.
    // It adds an 'i' when an error injection point is triggered, and adds an 'x' after a normal commit occurs.
    // For example, if the first error occurs between the prepare and commit, and the second error occurs immediately between the commit and the deletion of the system table, the err_events will contain consecutive "23".
    err_events: Vec<char>,
}

#[derive(Clone)]
pub struct TestSinkStore {
    inner: Arc<Mutex<TestSinkStoreInner>>,
}

impl TestSinkStore {
    pub fn new() -> Self {
        let max_record_error_count = 1024 * 1024 / 4;
        let err_events: Vec<char> = Vec::with_capacity(max_record_error_count);
        Self {
            inner: Arc::new(Mutex::new(TestSinkStoreInner {
                id_name: HashMap::new(),
                snapshot_id_to_data: BTreeMap::new(),
                epochs: Vec::new(),
                checkpoint_count: 0,
                err_count: 0,
                err_events,
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

    pub fn has_consecutive_error(&self, error_sequence: &str) -> bool {
        let events_str = self.get_events();
        events_str.contains(error_sequence)
    }

    fn get_events(&self) -> String {
        self.inner().err_events.iter().collect()
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
    err_rate_vec: Vec<Arc<AtomicU32>>,
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

#[async_trait]
impl CommitIceberg for CoordinatedTestWriter {
    async fn begin_txn(
        &mut self,
        commit_data: Vec<SinkMetadata>,
        snapshot_id: Option<i64>,
    ) -> risingwave_connector::sink::Result<i64> {
        if thread_rng().random_ratio(self.err_rate_vec[1].load(Relaxed), u32::MAX) {
            println!("Error injection point 1 -- Error occur before pre commit to meta store.");
            self.store.inc_err();
            self.store.inner().err_events.push('1');
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to pre commit to meta store"
            )));
        }
        let snapshot_id = match snapshot_id {
            Some(previous_snapshot_id) => previous_snapshot_id,
            None => generate_unique_snapshot_id(),
        };
        Ok(snapshot_id)
    }

    async fn do_commit(
        &mut self,
        commit_data: Vec<SinkMetadata>,
        snapshot_id: i64,
        commit_retry_num: u32,
    ) -> risingwave_connector::sink::Result<()> {

        if thread_rng().random_ratio(self.err_rate_vec[2].load(Relaxed), u32::MAX) {
            println!(
                "Error injection point 2 -- Error occur before commit to external sink and after pre commit."
            );
            self.store.inc_err();
            self.store.inner().err_events.push('2');
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to commit to external sink."
            )));
        }

        let file_ids = commit_data.into_iter().map(|metadata| {
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

        if thread_rng().random_ratio(self.err_rate_vec[3].load(Relaxed), u32::MAX) {
            println!(
                "Error injection point 3 -- Error occur after commit to external sink and before mark row deleted."
            );
            self.store.inc_err();
            self.store.inner().err_events.push('3');
            return Err(SinkError::Internal(anyhow::anyhow!(
                "fail to mark row deleted."
            )));
        }
        Ok(())
    }

    async fn is_snapshot_id_in_iceberg(
        &self,
        snapshot_id: i64,
    ) -> risingwave_connector::sink::Result<bool> {
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

fn generate_unique_snapshot_id() -> i64 {
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
    pub fn register_new_with_err_rate(err_rate_list: Vec<f64>) -> Self {
        let parallelism_counter = Arc::new(AtomicUsize::new(0));
        let mut err_rate_vec: Vec<Arc<AtomicU32>> = Vec::new();

        for err_rate in err_rate_list {
            let scaled_err_rate = (u32::MAX as f64 * err_rate) as u32;
            let arc_err_rate = Arc::new(AtomicU32::new(scaled_err_rate));
            err_rate_vec.push(arc_err_rate);
        }

        let err_rate_for_coordinator = Arc::new(AtomicU32::new(0));

        let store = TestSinkStore::new();

        let _sink_guard = {
            let staging_store = StagingDataStore::default();
            register_build_coordinated_sink(
                {
                    let parallelism_counter = parallelism_counter.clone();
                    let err_rate = err_rate_for_coordinator.clone();
                    let store = store.clone();
                    let err_rate_vec = err_rate_vec.clone();
                    let staging_store = staging_store.clone();
                    use risingwave_connector::sink::SinkWriterMetrics;
                    use risingwave_connector::sink::writer::SinkWriterExt;
                    move |param, writer_param| {
                        parallelism_counter.fetch_add(1, Relaxed);
                        let metrics = SinkWriterMetrics::new(&writer_param);
                        let writer = CoordinatedTestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                            err_rate_vec: err_rate_vec.clone(),
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
                    let err_rate = err_rate_for_coordinator.clone();
                    let err_rate_vec = err_rate_vec.clone();
                    let store = store.clone();
                    let staging_store = staging_store.clone();
                    let parallelism_counter = parallelism_counter.clone();
                    move |db, sink_param| {
                        let committer_inner = CoordinatedTestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                            err_rate_vec: err_rate_vec.clone(),
                            staging_store: staging_store.clone(),
                            staging: Default::default(),
                        };
                        Box::new(IcebergSinkCommitter {
                            commit_iceberg: committer_inner,
                            is_exactly_once: true,
                            last_commit_epoch: 0,
                            sink_id: sink_param.sink_id.sink_id(),
                            param: sink_param.clone(),
                            db,
                            commit_notifier: None,
                            _compact_task_guard: None,
                            commit_retry_num: 3,
                            committed_epoch_subscriber: None,
                        })
                    }
                },
            )
        };
        Self {
            _sink_guard,
            parallelism_counter: parallelism_counter.clone(),
            store,
            err_rate: err_rate_for_coordinator.clone(),
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
