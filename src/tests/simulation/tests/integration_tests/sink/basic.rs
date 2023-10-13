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

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;

use crate::sink::utils::{
    start_sink_test_cluster, SimulationTestSink, SimulationTestSource, CREATE_SINK, CREATE_SOURCE,
    DROP_SINK, DROP_SOURCE,
};

async fn basic_test_inner(is_decouple: bool) -> Result<()> {
    let mut cluster = start_sink_test_cluster().await?;

    let test_sink = SimulationTestSink::register_new();
    let test_source = SimulationTestSource::register_new(12, 0..1000000, 0.2, 50);

    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 6").await?;
    if is_decouple {
        session.run("set sink_decouple = true").await?;
    } else {
        session.run("set sink_decouple = false").await?;
    }
    session.run(CREATE_SOURCE).await?;
    session.run(CREATE_SINK).await?;
    assert_eq!(6, test_sink.parallelism_counter.load(Relaxed));

    test_sink
        .store
        .wait_for_count(test_source.id_list.len())
        .await;

    session.run(DROP_SINK).await?;
    session.run(DROP_SOURCE).await?;

    assert_eq!(0, test_sink.parallelism_counter.load(Relaxed));
    test_sink.store.check_simple_result(&test_source.id_list);
    assert!(test_sink.store.inner().checkpoint_count > 0);

    Ok(())
}

#[tokio::test]
async fn test_sink_basic() -> Result<()> {
    basic_test_inner(false).await
}

#[tokio::test]
async fn test_sink_decouple_basic() -> Result<()> {
    basic_test_inner(true).await
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
