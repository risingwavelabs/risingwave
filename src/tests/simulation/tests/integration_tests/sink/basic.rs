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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_connector::sink::boxed::{BoxCoordinator, BoxWriter};
use risingwave_connector::sink::test_sink::registry_build_sink;
use risingwave_connector::sink::{Sink, SinkWriter, SinkWriterParam};
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tokio::time::sleep;

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

struct TestSink {
    row_counter: Arc<AtomicUsize>,
    parallelism_counter: Arc<AtomicUsize>,
}

#[async_trait]
impl Sink for TestSink {
    type Coordinator = BoxCoordinator;
    type Writer = BoxWriter<()>;

    async fn validate(&self) -> risingwave_connector::sink::Result<()> {
        Ok(())
    }

    async fn new_writer(
        &self,
        _writer_param: SinkWriterParam,
    ) -> risingwave_connector::sink::Result<Self::Writer> {
        self.parallelism_counter.fetch_add(1, Relaxed);
        Ok(Box::new(TestWriter {
            parallelism_counter: self.parallelism_counter.clone(),
            row_counter: self.row_counter.clone(),
        }))
    }
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
        move |_param| {
            Ok(Box::new(TestSink {
                row_counter: row_counter.clone(),
                parallelism_counter: parallelism_counter.clone(),
            }))
        }
    });

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = false").await?;
    session
        .run("create table test_table (id int, name varchar)")
        .await?;
    session
        .run("create sink test_sink from test_table with (connector = 'test')")
        .await?;
    let mut count = 0;
    let mut id_list = (0..100000).collect_vec();
    id_list.shuffle(&mut rand::thread_rng());
    let flush_freq = 50;
    for id in &id_list[0..1000] {
        session
            .run(format!(
                "insert into test_table values ({}, 'name-{}')",
                id, id
            ))
            .await?;
        count += 1;
        if count % flush_freq == 0 {
            session.run("flush").await?;
        }
    }

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
        move |_param| {
            Ok(Box::new(TestSink {
                row_counter: row_counter.clone(),
                parallelism_counter: parallelism_counter.clone(),
            }))
        }
    });

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    session.run("set sink_decouple = true").await?;
    session
        .run("create table test_table (id int, name varchar)")
        .await?;
    session
        .run("create sink test_sink from test_table with (connector = 'test')")
        .await?;
    assert_eq!(6, parallelism_counter.load(Relaxed));

    let mut count = 0;
    let mut id_list = (0..100000).collect_vec();
    id_list.shuffle(&mut rand::thread_rng());
    let flush_freq = 50;
    for id in &id_list[0..1000] {
        session
            .run(format!(
                "insert into test_table values ({}, 'name-{}')",
                id, id
            ))
            .await?;
        count += 1;
        if count % flush_freq == 0 {
            session.run("flush").await?;
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
