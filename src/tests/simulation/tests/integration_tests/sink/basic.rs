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

use std::io::Write;
use std::iter::once;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::select_all;
use futures::StreamExt;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::boxed::{BoxCoordinator, BoxWriter};
use risingwave_connector::sink::test_sink::registry_build_sink;
use risingwave_connector::sink::writer::SinkWriter;
use risingwave_connector::sink::{Sink, SinkWriterParam};
use risingwave_connector::source::test_source::{registry_test_source, BoxSource, TestSourceSplit};
use risingwave_connector::source::StreamChunkWithState;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;

struct TestWriter {
    row_counter: Arc<AtomicUsize>,
    parallelism_counter: Arc<AtomicUsize>,
}

#[async_trait]
impl SinkWriter for TestWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> risingwave_connector::sink::Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> risingwave_connector::sink::Result<()> {
        let mut count = 0;
        for _ in chunk.rows() {
            count += 1;
        }
        self.row_counter.fetch_add(count, Relaxed);
        Ok(())
    }

    async fn barrier(
        &mut self,
        _is_checkpoint: bool,
    ) -> risingwave_connector::sink::Result<Self::CommitMetadata> {
        sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}

impl Drop for TestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

fn build_stream_chunk(row_iter: impl Iterator<Item = (i32, String)>) -> StreamChunk {
    let mut builder = DataChunkBuilder::new(vec![DataType::Int32, DataType::Varchar], 100000);
    for (id, name) in row_iter {
        assert!(builder
            .append_one_row([
                Some(ScalarImpl::Int32(id)),
                Some(ScalarImpl::Utf8(name.into())),
            ])
            .is_none());
    }
    let chunk = builder.consume_all().unwrap();
    let ops = (0..chunk.cardinality()).map(|_| Op::Insert).collect_vec();
    StreamChunk::from_parts(ops, chunk)
}

#[tokio::test]
async fn test_sink_basic() -> Result<()> {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    let mut cluster = Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        etcd_timeout_rate: 0.0,
        etcd_data_path: None,
    })
    .await?;

    let row_counter = Arc::new(AtomicUsize::new(0));
    let parallelism_counter = Arc::new(AtomicUsize::new(0));

    let _sink_guard = registry_build_sink({
        let row_counter = row_counter.clone();
        let parallelism_counter = parallelism_counter.clone();
        move |_, _| {
            parallelism_counter.fetch_add(1, Relaxed);
            Box::new(TestWriter {
                row_counter: row_counter.clone(),
                parallelism_counter: parallelism_counter.clone(),
            })
        }
    });

    let source_parallelism = 12;
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..source_parallelism {
        let (tx, rx): (_, UnboundedReceiver<StreamChunk>) = unbounded_channel();
        txs.push(tx);
        rxs.push(Some(rx));
    }

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
                let id: usize = split.id.parse().unwrap();
                let rx = rxs[id].take().unwrap();
                UnboundedReceiverStream::new(rx).map(|chunk| Ok(StreamChunkWithState::from(chunk)))
            }))
            .boxed()
        },
    ));

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = false").await?;
    session
        .run("create table test_table (id int primary key, name varchar) with (connector = 'test') FORMAT PLAIN ENCODE JSON")
        .await?;
    session
        .run("create sink test_sink from test_table with (connector = 'test')")
        .await?;
    let mut count = 0;
    let mut id_list: Vec<usize> = (0..100000).collect_vec();
    id_list.shuffle(&mut rand::thread_rng());
    let flush_freq = 50;
    for id in &id_list[0..10000] {
        let chunk = build_stream_chunk(once((*id as i32, format!("name-{}", id))));
        txs[id % source_parallelism].send(chunk).unwrap();
        count += 1;
        if count % flush_freq == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    }
    sleep(Duration::from_millis(10000)).await;

    assert_eq!(6, parallelism_counter.load(Relaxed));
    assert_eq!(count, row_counter.load(Relaxed));

    session.run("drop sink test_sink").await?;

    assert_eq!(0, parallelism_counter.load(Relaxed));

    Ok(())
}

#[tokio::test]
async fn test_sink_decouple_basic() -> Result<()> {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    let mut cluster = Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        etcd_timeout_rate: 0.0,
        etcd_data_path: None,
    })
    .await?;

    let row_counter = Arc::new(AtomicUsize::new(0));
    let parallelism_counter = Arc::new(AtomicUsize::new(0));

    let _sink_guard = registry_build_sink({
        let row_counter = row_counter.clone();
        let parallelism_counter = parallelism_counter.clone();
        move |_, _| {
            parallelism_counter.fetch_add(1, Relaxed);
            Box::new(TestWriter {
                row_counter: row_counter.clone(),
                parallelism_counter: parallelism_counter.clone(),
            })
        }
    });

    let source_parallelism = 12;
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..source_parallelism {
        let (tx, rx): (_, UnboundedReceiver<StreamChunk>) = unbounded_channel();
        txs.push(tx);
        rxs.push(Some(rx));
    }

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
                let id: usize = split.id.parse().unwrap();
                let rx = rxs[id].take().unwrap();
                UnboundedReceiverStream::new(rx).map(|chunk| Ok(StreamChunkWithState::from(chunk)))
            }))
            .boxed()
        },
    ));

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = true").await?;
    session
        .run("create table test_table (id int primary key, name varchar) with (connector = 'test') FORMAT PLAIN ENCODE JSON")
        .await?;
    session
        .run("create sink test_sink from test_table with (connector = 'test')")
        .await?;
    assert_eq!(6, parallelism_counter.load(Relaxed));

    let mut count = 0;
    let mut id_list = (0..100000).collect_vec();
    id_list.shuffle(&mut rand::thread_rng());
    let flush_freq = 50;
    for id in &id_list[0..10000] {
        let chunk = build_stream_chunk(once((*id as i32, format!("name-{}", id))));
        txs[id % source_parallelism].send(chunk).unwrap();
        count += 1;
        if count % flush_freq == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    }

    while row_counter.load(Relaxed) < count {
        sleep(Duration::from_millis(1000)).await
    }

    assert_eq!(count, row_counter.load(Relaxed));

    session.run("drop sink test_sink").await?;

    assert_eq!(0, parallelism_counter.load(Relaxed));

    Ok(())
}

#[tokio::test]
async fn test_sink_decouple_blackhole() -> Result<()> {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    let mut cluster = Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        etcd_timeout_rate: 0.0,
        etcd_data_path: None,
    })
    .await?;

    let source_parallelism = 12;
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..source_parallelism {
        let (tx, rx): (_, UnboundedReceiver<StreamChunk>) = unbounded_channel();
        txs.push(tx);
        rxs.push(Some(rx));
    }

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
                let id: usize = split.id.parse().unwrap();
                let rx = rxs[id].take().unwrap();
                UnboundedReceiverStream::new(rx).map(|chunk| Ok(StreamChunkWithState::from(chunk)))
            }))
            .boxed()
        },
    ));

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = true").await?;
    session
        .run("create table test_table (id int primary key, name varchar) with (connector = 'test') FORMAT PLAIN ENCODE JSON")
        .await?;
    session
        .run("create sink test_sink from test_table with (connector = 'blackhole')")
        .await?;

    let mut count = 0;
    let mut id_list = (0..100000).collect_vec();
    id_list.shuffle(&mut rand::thread_rng());
    let flush_freq = 50;
    for id in &id_list[0..10000] {
        let chunk = build_stream_chunk(once((*id as i32, format!("name-{}", id))));
        txs[id % source_parallelism].send(chunk).unwrap();
        count += 1;
        if count % flush_freq == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    }

    session.run("drop sink test_sink").await?;
    Ok(())
}
