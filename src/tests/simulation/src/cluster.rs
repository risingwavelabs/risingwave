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

#![cfg_attr(not(madsim), allow(unused_imports))]

use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use cfg_or_panic::cfg_or_panic;
use clap::Parser;
use futures::channel::{mpsc, oneshot};
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
#[cfg(madsim)]
use madsim::runtime::{Handle, NodeHandle};
use rand::seq::IteratorRandom;
use rand::Rng;
use risingwave_pb::common::WorkerNode;
use sqllogictest::AsyncDB;
#[cfg(not(madsim))]
use tokio::runtime::Handle;

use crate::client::RisingWave;

/// The path to the configuration file for the cluster.
#[derive(Clone, Debug)]
pub enum ConfigPath {
    /// A regular path pointing to a external configuration file.
    Regular(String),
    /// A temporary path pointing to a configuration file created at runtime.
    Temp(Arc<tempfile::TempPath>),
}

impl ConfigPath {
    pub fn as_str(&self) -> &str {
        match self {
            ConfigPath::Regular(s) => s,
            ConfigPath::Temp(p) => p.as_os_str().to_str().unwrap(),
        }
    }
}

/// RisingWave cluster configuration.
#[derive(Debug, Clone)]
pub struct Configuration {
    /// The path to configuration file.
    ///
    /// Empty string means using the default config.
    pub config_path: ConfigPath,

    /// The number of frontend nodes.
    pub frontend_nodes: usize,

    /// The number of compute nodes.
    pub compute_nodes: usize,

    /// The number of meta nodes.
    pub meta_nodes: usize,

    /// The number of compactor nodes.
    pub compactor_nodes: usize,

    /// The number of CPU cores for each compute node.
    ///
    /// This determines worker_node_parallelism.
    pub compute_node_cores: usize,

    /// The probability of etcd request timeout.
    pub etcd_timeout_rate: f32,

    /// Path to etcd data file.
    pub etcd_data_path: Option<PathBuf>,
}

impl Configuration {
    /// Returns the config for scale test.
    pub fn for_scale() -> Self {
        // Embed the config file and create a temporary file at runtime. The file will be deleted
        // automatically when it's dropped.
        let config_path = {
            let mut file =
                tempfile::NamedTempFile::new().expect("failed to create temp config file");
            file.write_all(include_bytes!("risingwave-scale.toml"))
                .expect("failed to write config file");
            file.into_temp_path()
        };

        Configuration {
            config_path: ConfigPath::Temp(config_path.into()),
            frontend_nodes: 2,
            compute_nodes: 3,
            meta_nodes: 3,
            compactor_nodes: 2,
            compute_node_cores: 2,
            etcd_timeout_rate: 0.0,
            etcd_data_path: None,
        }
    }

    /// Returns the config for backfill test.
    pub fn for_backfill() -> Self {
        // Embed the config file and create a temporary file at runtime. The file will be deleted
        // automatically when it's dropped.
        let config_path = {
            let mut file =
                tempfile::NamedTempFile::new().expect("failed to create temp config file");
            file.write_all(include_bytes!("backfill.toml"))
                .expect("failed to write config file");
            file.into_temp_path()
        };

        Configuration {
            config_path: ConfigPath::Temp(config_path.into()),
            frontend_nodes: 1,
            compute_nodes: 1,
            meta_nodes: 1,
            compactor_nodes: 1,
            compute_node_cores: 4,
            etcd_timeout_rate: 0.0,
            etcd_data_path: None,
        }
    }

    pub fn for_background_ddl() -> Self {
        // Embed the config file and create a temporary file at runtime. The file will be deleted
        // automatically when it's dropped.
        let config_path = {
            let mut file =
                tempfile::NamedTempFile::new().expect("failed to create temp config file");
            file.write_all(include_bytes!("background_ddl.toml"))
                .expect("failed to write config file");
            file.into_temp_path()
        };

        Configuration {
            config_path: ConfigPath::Temp(config_path.into()),
            frontend_nodes: 2,
            compute_nodes: 3,
            meta_nodes: 3,
            compactor_nodes: 2,
            compute_node_cores: 2,
            etcd_timeout_rate: 0.0,
            etcd_data_path: None,
        }
    }
}

/// A risingwave cluster.
///
/// # Nodes
///
/// | Name           | IP            |
/// | -------------- | ------------- |
/// | meta-x         | 192.168.1.x   |
/// | frontend-x     | 192.168.2.x   |
/// | compute-x      | 192.168.3.x   |
/// | compactor-x    | 192.168.4.x   |
/// | etcd           | 192.168.10.1  |
/// | kafka-broker   | 192.168.11.1  |
/// | kafka-producer | 192.168.11.2  |
/// | s3             | 192.168.12.1  |
/// | client         | 192.168.100.1 |
/// | ctl            | 192.168.101.1 |
pub struct Cluster {
    config: Configuration,
    handle: Handle,
    #[cfg(madsim)]
    pub(crate) client: NodeHandle,
    #[cfg(madsim)]
    pub(crate) ctl: NodeHandle,
}

impl Cluster {
    /// Start a RisingWave cluster for testing.
    ///
    /// This function should be called exactly once in a test.
    #[cfg_or_panic(madsim)]
    pub async fn start(conf: Configuration) -> Result<Self> {
        use madsim::net::ipvs::*;

        let handle = madsim::runtime::Handle::current();
        println!("seed = {}", handle.seed());
        println!("{:#?}", conf);

        // setup DNS and load balance
        let net = madsim::net::NetSim::current();
        net.add_dns_record("etcd", "192.168.10.1".parse().unwrap());
        for i in 1..=conf.meta_nodes {
            net.add_dns_record(
                &format!("meta-{i}"),
                format!("192.168.1.{i}").parse().unwrap(),
            );
        }

        net.add_dns_record("frontend", "192.168.2.0".parse().unwrap());
        net.add_dns_record("message_queue", "192.168.11.1".parse().unwrap());
        net.global_ipvs().add_service(
            ServiceAddr::Tcp("192.168.2.0:4566".into()),
            Scheduler::RoundRobin,
        );
        for i in 1..=conf.frontend_nodes {
            net.global_ipvs().add_server(
                ServiceAddr::Tcp("192.168.2.0:4566".into()),
                &format!("192.168.2.{i}:4566"),
            )
        }

        // etcd node
        let etcd_data = conf
            .etcd_data_path
            .as_ref()
            .map(|path| std::fs::read_to_string(path).unwrap());
        handle
            .create_node()
            .name("etcd")
            .ip("192.168.10.1".parse().unwrap())
            .init(move || {
                let addr = "0.0.0.0:2388".parse().unwrap();
                let mut builder =
                    etcd_client::SimServer::builder().timeout_rate(conf.etcd_timeout_rate);
                if let Some(data) = &etcd_data {
                    builder = builder.load(data.clone());
                }
                builder.serve(addr)
            })
            .build();

        // kafka broker
        handle
            .create_node()
            .name("kafka-broker")
            .ip("192.168.11.1".parse().unwrap())
            .init(move || async move {
                rdkafka::SimBroker::default()
                    .serve("0.0.0.0:29092".parse().unwrap())
                    .await
            })
            .build();

        // s3
        handle
            .create_node()
            .name("s3")
            .ip("192.168.12.1".parse().unwrap())
            .init(move || async move {
                aws_sdk_s3::server::SimServer::default()
                    .with_bucket("hummock001")
                    .serve("0.0.0.0:9301".parse().unwrap())
                    .await
            })
            .build();

        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let mut meta_addrs = vec![];
        for i in 1..=conf.meta_nodes {
            meta_addrs.push(format!("http://meta-{i}:5690"));
        }
        std::env::set_var("RW_META_ADDR", meta_addrs.join(","));

        // meta node
        for i in 1..=conf.meta_nodes {
            let opts = risingwave_meta_node::MetaNodeOpts::parse_from([
                "meta-node",
                "--config-path",
                conf.config_path.as_str(),
                "--listen-addr",
                "0.0.0.0:5690",
                "--advertise-addr",
                &format!("meta-{i}:5690"),
                "--backend",
                "etcd",
                "--etcd-endpoints",
                "etcd:2388",
                "--state-store",
                "hummock+minio://hummockadmin:hummockadmin@192.168.12.1:9301/hummock001",
                "--data-directory",
                "hummock_001",
            ]);
            handle
                .create_node()
                .name(format!("meta-{i}"))
                .ip([192, 168, 1, i as u8].into())
                .init(move || risingwave_meta_node::start(opts.clone()))
                .build();
        }

        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        // frontend node
        for i in 1..=conf.frontend_nodes {
            let opts = risingwave_frontend::FrontendOpts::parse_from([
                "frontend-node",
                "--config-path",
                conf.config_path.as_str(),
                "--listen-addr",
                "0.0.0.0:4566",
                "--advertise-addr",
                &format!("192.168.2.{i}:4566"),
            ]);
            handle
                .create_node()
                .name(format!("frontend-{i}"))
                .ip([192, 168, 2, i as u8].into())
                .init(move || risingwave_frontend::start(opts.clone()))
                .build();
        }

        // compute node
        for i in 1..=conf.compute_nodes {
            let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                "compute-node",
                "--config-path",
                conf.config_path.as_str(),
                "--listen-addr",
                "0.0.0.0:5688",
                "--advertise-addr",
                &format!("192.168.3.{i}:5688"),
                "--total-memory-bytes",
                "6979321856",
                "--parallelism",
                &conf.compute_node_cores.to_string(),
            ]);
            handle
                .create_node()
                .name(format!("compute-{i}"))
                .ip([192, 168, 3, i as u8].into())
                .cores(conf.compute_node_cores)
                .init(move || risingwave_compute::start(opts.clone()))
                .build();
        }

        // compactor node
        for i in 1..=conf.compactor_nodes {
            let opts = risingwave_compactor::CompactorOpts::parse_from([
                "compactor-node",
                "--config-path",
                conf.config_path.as_str(),
                "--listen-addr",
                "0.0.0.0:6660",
                "--advertise-addr",
                &format!("192.168.4.{i}:6660"),
            ]);
            handle
                .create_node()
                .name(format!("compactor-{i}"))
                .ip([192, 168, 4, i as u8].into())
                .init(move || risingwave_compactor::start(opts.clone()))
                .build();
        }

        // wait for the service to be ready
        tokio::time::sleep(Duration::from_secs(15)).await;

        // client
        let client = handle
            .create_node()
            .name("client")
            .ip([192, 168, 100, 1].into())
            .build();

        // risectl
        let ctl = handle
            .create_node()
            .name("ctl")
            .ip([192, 168, 101, 1].into())
            .build();

        Ok(Self {
            config: conf,
            handle,
            client,
            ctl,
        })
    }

    /// Start a SQL session on the client node.
    #[cfg_or_panic(madsim)]
    pub fn start_session(&mut self) -> Session {
        let (query_tx, mut query_rx) = mpsc::channel::<SessionRequest>(0);

        self.client.spawn(async move {
            let mut client = RisingWave::connect("frontend".into(), "dev".into()).await?;

            while let Some((sql, tx)) = query_rx.next().await {
                let result = client
                    .run(&sql)
                    .await
                    .map(|output| match output {
                        sqllogictest::DBOutput::Rows { rows, .. } => rows
                            .into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            })
                            .collect::<Vec<_>>()
                            .join("\n"),
                        _ => "".to_string(),
                    })
                    .map_err(Into::into);

                let _ = tx.send(result);
            }

            Ok::<_, anyhow::Error>(())
        });

        Session { query_tx }
    }

    /// Run a SQL query on a **new** session of the client node.
    ///
    /// This is a convenience method that creates a new session and runs the query on it. If you
    /// want to run multiple queries on the same session, use `start_session` and `Session::run`.
    pub async fn run(&mut self, sql: impl Into<String>) -> Result<String> {
        self.start_session().run(sql).await
    }

    /// Run a future on the client node.
    #[cfg_or_panic(madsim)]
    pub async fn run_on_client<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.client.spawn(future).await.unwrap()
    }

    pub async fn get_random_worker_nodes(&self, n: usize) -> Result<Vec<WorkerNode>> {
        let worker_nodes = self.get_cluster_info().await?.get_worker_nodes().clone();
        if worker_nodes.len() < n {
            return Err(anyhow!("cannot remove more nodes than present"));
        }
        let rand_nodes = worker_nodes
            .iter()
            .choose_multiple(&mut rand::thread_rng(), n)
            .to_vec();
        Ok(rand_nodes.iter().cloned().cloned().collect_vec())
    }

    /// Run a SQL query from the client and wait until the condition is met.
    pub async fn wait_until(
        &mut self,
        sql: impl Into<String> + Clone,
        mut p: impl FnMut(&str) -> bool,
        interval: Duration,
        timeout: Duration,
    ) -> Result<String> {
        let fut = async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                let result = self.run(sql.clone()).await?;
                if p(&result) {
                    return Ok::<_, anyhow::Error>(result);
                }
            }
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => Ok(r?),
            Err(_) => bail!("wait_until timeout"),
        }
    }

    /// Run a SQL query from the client and wait until the return result is not empty.
    pub async fn wait_until_non_empty(
        &mut self,
        sql: &str,
        interval: Duration,
        timeout: Duration,
    ) -> Result<String> {
        self.wait_until(sql, |r| !r.trim().is_empty(), interval, timeout)
            .await
    }

    /// Generate a list of random worker nodes to kill by `opts`, then call `kill_nodes` to kill and
    /// restart them.
    pub async fn kill_node(&self, opts: &KillOpts) {
        let mut nodes = vec![];
        if opts.kill_meta {
            let rand = rand::thread_rng().gen_range(0..3);
            for i in 1..=self.config.meta_nodes {
                match rand {
                    0 => break,                                         // no killed
                    1 => {}                                             // all killed
                    _ if !rand::thread_rng().gen_bool(0.5) => continue, // random killed
                    _ => {}
                }
                nodes.push(format!("meta-{}", i));
            }
            // don't kill all meta services
            if nodes.len() == self.config.meta_nodes {
                nodes.truncate(1);
            }
        }
        if opts.kill_frontend {
            let rand = rand::thread_rng().gen_range(0..3);
            for i in 1..=self.config.frontend_nodes {
                match rand {
                    0 => break,                                         // no killed
                    1 => {}                                             // all killed
                    _ if !rand::thread_rng().gen_bool(0.5) => continue, // random killed
                    _ => {}
                }
                nodes.push(format!("frontend-{}", i));
            }
        }
        if opts.kill_compute {
            let rand = rand::thread_rng().gen_range(0..3);
            for i in 1..=self.config.compute_nodes {
                match rand {
                    0 => break,                                         // no killed
                    1 => {}                                             // all killed
                    _ if !rand::thread_rng().gen_bool(0.5) => continue, // random killed
                    _ => {}
                }
                nodes.push(format!("compute-{}", i));
            }
        }
        if opts.kill_compactor {
            let rand = rand::thread_rng().gen_range(0..3);
            for i in 1..=self.config.compactor_nodes {
                match rand {
                    0 => break,                                         // no killed
                    1 => {}                                             // all killed
                    _ if !rand::thread_rng().gen_bool(0.5) => continue, // random killed
                    _ => {}
                }
                nodes.push(format!("compactor-{}", i));
            }
        }

        self.kill_nodes(nodes, opts.restart_delay_secs).await
    }

    /// Kill the given nodes by their names and restart them in 2s + restart_delay_secs with a
    /// probability of 0.1.
    #[cfg_or_panic(madsim)]
    pub async fn kill_nodes(
        &self,
        nodes: impl IntoIterator<Item = impl AsRef<str>>,
        restart_delay_secs: u32,
    ) {
        join_all(nodes.into_iter().map(|name| async move {
            let name = name.as_ref();
            let t = rand::thread_rng().gen_range(Duration::from_secs(0)..Duration::from_secs(1));
            tokio::time::sleep(t).await;
            tracing::info!("kill {name}");
            Handle::current().kill(name);

            let mut t =
                rand::thread_rng().gen_range(Duration::from_secs(0)..Duration::from_secs(1));
            // has a small chance to restart after a long time
            // so that the node is expired and removed from the cluster
            if rand::thread_rng().gen_bool(0.1) {
                // max_heartbeat_interval_secs = 15
                t += Duration::from_secs(restart_delay_secs as u64);
            }
            tokio::time::sleep(t).await;
            tracing::info!("restart {name}");
            Handle::current().restart(name);
        }))
        .await;
    }

    /// Create a node for kafka producer and prepare data.
    #[cfg_or_panic(madsim)]
    pub async fn create_kafka_producer(&self, datadir: &str) {
        self.handle
            .create_node()
            .name("kafka-producer")
            .ip("192.168.11.2".parse().unwrap())
            .build()
            .spawn(crate::kafka::producer(
                "192.168.11.1:29092",
                datadir.to_string(),
            ))
            .await
            .unwrap();
    }

    /// Create a kafka topic.
    #[cfg_or_panic(madsim)]
    pub fn create_kafka_topics(&self, topics: HashMap<String, i32>) {
        self.handle
            .create_node()
            .name("kafka-topic-create")
            .ip("192.168.11.3".parse().unwrap())
            .build()
            .spawn(crate::kafka::create_topics("192.168.11.1:29092", topics));
    }

    pub fn config(&self) -> Configuration {
        self.config.clone()
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Graceful shutdown all RisingWave nodes.
    #[cfg_or_panic(madsim)]
    pub async fn graceful_shutdown(&self) {
        let mut nodes = vec![];
        let mut metas = vec![];
        for i in 1..=self.config.meta_nodes {
            metas.push(format!("meta-{i}"));
        }
        for i in 1..=self.config.frontend_nodes {
            nodes.push(format!("frontend-{i}"));
        }
        for i in 1..=self.config.compute_nodes {
            nodes.push(format!("compute-{i}"));
        }
        for i in 1..=self.config.compactor_nodes {
            nodes.push(format!("compactor-{i}"));
        }

        tracing::info!("graceful shutdown");
        let waiting_time = Duration::from_secs(10);
        // shutdown frontends, computes, compactors
        for node in &nodes {
            self.handle.send_ctrl_c(node);
        }
        tokio::time::sleep(waiting_time).await;
        // shutdown metas
        for meta in &metas {
            self.handle.send_ctrl_c(meta);
        }
        tokio::time::sleep(waiting_time).await;

        // check all nodes are exited
        for node in nodes.iter().chain(metas.iter()) {
            if !self.handle.is_exit(node) {
                panic!("failed to graceful shutdown {node} in {waiting_time:?}");
            }
        }
    }
}

type SessionRequest = (
    String,                          // query sql
    oneshot::Sender<Result<String>>, // channel to send result back
);

/// A SQL session on the simulated client node.
#[derive(Debug, Clone)]
pub struct Session {
    query_tx: mpsc::Sender<SessionRequest>,
}

impl Session {
    /// Run the given SQL query on the session.
    pub async fn run(&mut self, sql: impl Into<String>) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.query_tx.send((sql.into(), tx)).await?;
        rx.await?
    }

    /// Run `FLUSH` on the session.
    pub async fn flush(&mut self) -> Result<()> {
        self.run("FLUSH").await?;
        Ok(())
    }
}

/// Options for killing nodes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KillOpts {
    pub kill_rate: f32,
    pub kill_meta: bool,
    pub kill_frontend: bool,
    pub kill_compute: bool,
    pub kill_compactor: bool,
    pub restart_delay_secs: u32,
}

impl KillOpts {
    /// Killing all kind of nodes.
    pub const ALL: Self = KillOpts {
        kill_rate: 1.0,
        kill_meta: true,
        kill_frontend: true,
        kill_compute: true,
        kill_compactor: true,
        restart_delay_secs: 20,
    };
    pub const ALL_FAST: Self = KillOpts {
        kill_rate: 1.0,
        kill_meta: true,
        kill_frontend: true,
        kill_compute: true,
        kill_compactor: true,
        restart_delay_secs: 2,
    };
}
