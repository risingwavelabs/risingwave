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

use std::collections::HashMap;
use std::io::Write;
use std::iter::once;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicUsize};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

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
use risingwave_connector::sink::SinkError;
use risingwave_connector::sink::test_sink::{TestSinkRegistryGuard, registry_build_sink};
use risingwave_connector::sink::writer::SinkWriter;
use risingwave_connector::source::test_source::{
    BoxSource, TestSourceRegistryGuard, TestSourceSplit, registry_test_source,
};
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tokio::task::yield_now;
use tokio::time::sleep;

use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

pub const CREATE_SOURCE: &str = "create source test_source (id int, name varchar) with (connector = 'test') FORMAT PLAIN ENCODE JSON";
pub const CREATE_SINK: &str =
    "create sink test_sink from test_source with (connector = 'test', type = 'upsert')";
pub const DROP_SINK: &str = "drop sink test_sink";
pub const DROP_SOURCE: &str = "drop source test_source";

pub struct TestSinkStoreInner {
    pub id_name: HashMap<i32, Vec<String>>,
    pub epochs: Vec<u64>,
    pub checkpoint_count: usize,
    pub err_count: usize,
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
                err_count: 0,
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

    pub fn err_count(&self) -> usize {
        self.inner().err_count
    }

    pub async fn wait_for_count(&self, count: usize) -> anyhow::Result<()> {
        let mut prev_count = 0;
        let mut has_printed = false;
        loop {
            sleep(Duration::from_secs(1)).await;
            let curr_count = self.id_count();
            if curr_count >= count {
                assert_eq!(count, curr_count);
                break;
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

impl TestWriter {
    pub fn new(
        store: TestSinkStore,
        parallelism_counter: Arc<AtomicUsize>,
        err_rate: Arc<AtomicU32>,
    ) -> Self {
        Self {
            store,
            parallelism_counter,
            err_rate,
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
        if thread_rng().random_ratio(self.err_rate.load(Relaxed), u32::MAX) {
            println!("write with err");
            self.store.inner().err_count += 1;
            return Err(SinkError::Internal(anyhow::anyhow!("fail to write")));
        }
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
    pub err_rate: Arc<AtomicU32>,
}

impl SimulationTestSink {
    pub fn register_new() -> Self {
        let parallelism_counter = Arc::new(AtomicUsize::new(0));
        let err_rate = Arc::new(AtomicU32::new(0));
        let store = TestSinkStore::new();

        let _sink_guard = registry_build_sink({
            let parallelism_counter = parallelism_counter.clone();
            let err_rate = err_rate.clone();
            let store = store.clone();
            use risingwave_connector::sink::SinkWriterMetrics;
            use risingwave_connector::sink::writer::SinkWriterExt;
            move |_, writer_param| {
                parallelism_counter.fetch_add(1, Relaxed);
                let metrics = SinkWriterMetrics::new(&writer_param);
                risingwave_connector::sink::boxed::boxed_log_sinker(
                    TestWriter::new(store.clone(), parallelism_counter.clone(), err_rate.clone())
                        .into_log_sinker(metrics),
                )
            }
        });

        Self {
            _sink_guard,
            parallelism_counter,
            store,
            err_rate,
        }
    }

    pub fn set_err_rate(&self, err_rate: f64) {
        let err_rate = u32::MAX as f64 * err_rate;
        self.err_rate.store(err_rate as _, Relaxed);
    }

    pub async fn wait_initial_parallelism(&self, parallelism: usize) -> Result<()> {
        while self.parallelism_counter.load(Relaxed) < parallelism {
            yield_now().await;
        }
        assert_eq!(self.parallelism_counter.load(Relaxed), parallelism);
        Ok(())
    }
}

pub fn build_stream_chunk(
    row_iter: impl Iterator<Item = (i32, String, String, String)>,
) -> StreamChunk {
    static ROW_ID_GEN: LazyLock<Arc<AtomicI64>> = LazyLock::new(|| Arc::new(AtomicI64::new(0)));

    let mut builder = DataChunkBuilder::new(
        vec![
            DataType::Int32,
            DataType::Varchar,
            DataType::Serial,
            DataType::Varchar,
            DataType::Varchar,
        ],
        100000,
    );
    for (id, name, split_id, offset) in row_iter {
        let row_id = ROW_ID_GEN.fetch_add(1, Relaxed);
        std::assert!(
            builder
                .append_one_row([
                    Some(ScalarImpl::Int32(id)),
                    Some(ScalarImpl::Utf8(name.into())),
                    Some(ScalarImpl::Serial(Serial::from(row_id))),
                    Some(ScalarImpl::Utf8(split_id.into())),
                    Some(ScalarImpl::Utf8(offset.into())),
                ])
                .is_none()
        );
    }
    let chunk = builder.consume_all().unwrap();
    let ops = (0..chunk.cardinality()).map(|_| Op::Insert).collect_vec();
    StreamChunk::from_parts(ops, chunk)
}

pub struct SimulationTestSource {
    _source_guard: TestSourceRegistryGuard,
    pub id_list: Vec<i32>,
    pub create_stream_count: Arc<AtomicUsize>,
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
        id_list.shuffle(&mut rand::rng());
        let id_list = id_list[0..count].iter().cloned().collect_vec();
        let mut id_lists = vec![vec![]; source_parallelism];
        for id in &id_list {
            id_lists[*id as usize % source_parallelism].push(*id);
        }
        let create_stream_count = Arc::new(AtomicUsize::new(0));
        let id_lists_clone = id_lists.iter().map(|l| Arc::new(l.clone())).collect_vec();
        let box_source_create_stream_count = create_stream_count.clone();
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
                box_source_create_stream_count.fetch_add(1, Relaxed);
                select_all(splits.into_iter().map(|split| {
                    let split_id: usize = split.id.parse().unwrap();
                    let id_list = id_lists_clone[split_id].clone();
                    let mut offset = if split.offset == "" {
                        0
                    } else {
                        split.offset.parse::<usize>().unwrap() + 1
                    };

                    let mut stream: BoxStream<'static, StreamChunk> = empty().boxed();

                    while offset < id_list.len() {
                        let mut chunks = Vec::new();
                        while offset < id_list.len() && chunks.len() < pause_interval {
                            let id = id_list[offset];
                            let chunk = build_stream_chunk(once((
                                id,
                                simple_name_of_id(id),
                                split.id.to_string(),
                                offset.to_string(),
                            )));
                            chunks.push(chunk);

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
                        .chain(async { pending::<StreamChunk>().await }.into_stream())
                        .map(|chunk| Ok(chunk))
                        .boxed()
                }))
                .boxed()
            },
        ));

        Self {
            _source_guard,
            id_list,
            create_stream_count,
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
        ..Default::default()
    })
    .await
}
