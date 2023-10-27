// Copyright 2023 RisingWave Labs
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

use std::collections::HashMap;
use std::io::Write;
use std::iter::once;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use futures::future::pending;
use futures::stream::{empty, select_all, BoxStream};
use futures::{stream, FutureExt, StreamExt};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarImpl, Serial};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::test_sink::{registry_build_sink, TestSinkRegistryGuard};
use risingwave_connector::sink::writer::SinkWriter;
use risingwave_connector::source::test_source::{
    registry_test_source, BoxSource, TestSourceRegistryGuard, TestSourceSplit,
};
use risingwave_connector::source::StreamChunkWithState;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tokio::time::sleep;

use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

pub const CREATE_SOURCE: &str = "create source test_source (id int, name varchar) with (connector = 'test') FORMAT PLAIN ENCODE JSON";
pub const CREATE_SINK: &str = "create sink test_sink from test_source with (connector = 'test')";
pub const DROP_SINK: &str = "drop sink test_sink";
pub const DROP_SOURCE: &str = "drop source test_source";

pub struct TestSinkStoreInner {
    pub id_name: HashMap<i32, Vec<String>>,
    pub epochs: Vec<u64>,
    pub checkpoint_count: usize,
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
                epochs: Vec::new(),
                checkpoint_count: 0,
            })),
        }
    }

    pub fn insert(&self, id: i32, name: String) {
        self.inner().id_name.entry(id).or_default().push(name);
    }

    pub fn begin_epoch(&self, epoch: u64) {
        self.inner().epochs.push(epoch)
    }

    pub fn inner(&self) -> MutexGuard<'_, TestSinkStoreInner> {
        self.inner.lock().unwrap()
    }

    pub fn check_simple_result(&self, id_list: &[i32]) -> anyhow::Result<()> {
        let inner = self.inner();
        assert_eq!(inner.id_name.len(), id_list.len());
        for id in id_list {
            let names = inner.id_name.get(id).unwrap();
            assert!(!names.is_empty());
            for name in names {
                assert_eq!(name, &simple_name_of_id(*id));
            }
        }
        Ok(())
    }

    pub fn id_count(&self) -> usize {
        self.inner().id_name.len()
    }

    pub async fn wait_for_count(&self, count: usize) -> anyhow::Result<()> {
        let mut prev_count = 0;
        loop {
            sleep(Duration::from_secs(1)).await;
            let curr_count = self.id_count();
            if curr_count >= count {
                assert_eq!(count, curr_count);
                break;
            }
            assert!(
                curr_count > prev_count,
                "not making progress: curr {}, prev {}",
                curr_count,
                prev_count
            );
            prev_count = curr_count;
        }
        Ok(())
    }
}

pub struct TestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
}

impl TestWriter {
    pub fn new(store: TestSinkStore, parallelism_counter: Arc<AtomicUsize>) -> Self {
        Self {
            store,
            parallelism_counter,
        }
    }
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
            self.store.inner().checkpoint_count += 1;
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

pub fn simple_name_of_id(id: i32) -> String {
    format!("name-{}", id)
}

pub struct SimulationTestSink {
    _sink_guard: TestSinkRegistryGuard,
    pub store: TestSinkStore,
    pub parallelism_counter: Arc<AtomicUsize>,
}

impl SimulationTestSink {
    pub fn register_new() -> Self {
        let parallelism_counter = Arc::new(AtomicUsize::new(0));
        let store = TestSinkStore::new();

        let _sink_guard = registry_build_sink({
            let parallelism_counter = parallelism_counter.clone();
            let store = store.clone();
            move |_, _| {
                parallelism_counter.fetch_add(1, Relaxed);
                Box::new(TestWriter::new(store.clone(), parallelism_counter.clone()))
            }
        });

        Self {
            _sink_guard,
            parallelism_counter,
            store,
        }
    }
}

pub fn build_stream_chunk(row_iter: impl Iterator<Item = (i32, String)>) -> StreamChunk {
    static ROW_ID_GEN: LazyLock<Arc<AtomicI64>> = LazyLock::new(|| Arc::new(AtomicI64::new(0)));

    let mut builder = DataChunkBuilder::new(
        vec![DataType::Int32, DataType::Varchar, DataType::Serial],
        100000,
    );
    for (id, name) in row_iter {
        let row_id = ROW_ID_GEN.fetch_add(1, Relaxed);
        std::assert!(builder
            .append_one_row([
                Some(ScalarImpl::Int32(id)),
                Some(ScalarImpl::Utf8(name.into())),
                Some(ScalarImpl::Serial(Serial::from(row_id))),
            ])
            .is_none());
    }
    let chunk = builder.consume_all().unwrap();
    let ops = (0..chunk.cardinality()).map(|_| Op::Insert).collect_vec();
    StreamChunk::from_parts(ops, chunk)
}

pub struct SimulationTestSource {
    _source_guard: TestSourceRegistryGuard,
    pub id_list: Vec<i32>,
}

impl SimulationTestSource {
    pub fn register_new(
        source_parallelism: usize,
        id_list: impl Iterator<Item = i32>,
        sample_rate: f32,
        pause_interval: usize,
    ) -> Self {
        let mut id_list: Vec<i32> = id_list.collect_vec();
        let count = (id_list.len() as f32 * sample_rate) as usize;
        id_list.shuffle(&mut rand::thread_rng());
        let id_list = id_list[0..count].iter().cloned().collect_vec();
        let mut id_lists = vec![vec![]; source_parallelism];
        for id in &id_list {
            id_lists[*id as usize % source_parallelism].push(*id);
        }
        let id_lists_clone = id_lists.iter().map(|l| Arc::new(l.clone())).collect_vec();
        let _source_guard = registry_test_source(BoxSource::new(
            move |_, _| {
                Ok((0..source_parallelism)
                    .map(|i: usize| TestSourceSplit {
                        id: format!("{}", i).as_str().into(),
                        properties: Default::default(),
                        offset: "".to_string(),
                    })
                    .collect_vec())
            },
            move |_, splits, _, _, _| {
                select_all(splits.into_iter().map(|split| {
                    let split_id: usize = split.id.parse().unwrap();
                    let id_list = id_lists_clone[split_id].clone();
                    let mut offset = if split.offset == "" {
                        0
                    } else {
                        split.offset.parse::<usize>().unwrap() + 1
                    };

                    let mut stream: BoxStream<'static, StreamChunkWithState> = empty().boxed();

                    while offset < id_list.len() {
                        let mut chunks = Vec::new();
                        while offset < id_list.len() && chunks.len() < pause_interval {
                            let id = id_list[offset];
                            let chunk = build_stream_chunk(once((id, simple_name_of_id(id))));
                            let mut split_offset = HashMap::new();
                            split_offset.insert(split.id.clone(), offset.to_string());
                            let chunk_with_state = StreamChunkWithState {
                                chunk,
                                split_offset_mapping: Some(split_offset),
                            };
                            chunks.push(chunk_with_state);

                            offset += 1;
                        }

                        stream = stream
                            .chain(
                                async move { stream::iter(chunks) }
                                    .into_stream()
                                    .chain(
                                        async move {
                                            sleep(Duration::from_millis(100)).await;
                                            stream::iter(Vec::new())
                                        }
                                        .into_stream(),
                                    )
                                    .flatten(),
                            )
                            .boxed();
                    }

                    stream
                        .chain(async { pending::<StreamChunkWithState>().await }.into_stream())
                        .map(|chunk| Ok(chunk))
                        .boxed()
                }))
                .boxed()
            },
        ));

        Self {
            _source_guard,
            id_list,
        }
    }
}

pub async fn start_sink_test_cluster() -> Result<Cluster> {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        etcd_timeout_rate: 0.0,
        etcd_data_path: None,
    })
    .await
}
