// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::io::Write;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::Parser;
use futures::future::{join_all, BoxFuture};
use madsim::runtime::{Handle, NodeHandle};
use rand::Rng;
use sqllogictest::AsyncDB;

use crate::client::RisingWave;

/// Embed the config file and create a temporary file at runtime.
static CONFIG_PATH: LazyLock<tempfile::TempPath> = LazyLock::new(|| {
    let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
    file.write_all(include_bytes!("risingwave.toml"))
        .expect("failed to write config file");
    file.into_temp_path()
});

#[derive(Debug, Clone, Copy)]
pub struct Configuration {
    /// The number of frontend nodes.
    pub frontend_nodes: usize,

    /// The number of compute nodes.
    pub compute_nodes: usize,

    /// The number of compactor nodes.
    pub compactor_nodes: usize,

    /// The number of CPU cores for each compute node.
    ///
    /// This determines worker_node_parallelism.
    pub compute_node_cores: usize,

    /// The probability of etcd request timeout.
    pub etcd_timeout_rate: f32,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            frontend_nodes: 2,
            compute_nodes: 3,
            compactor_nodes: 2,
            compute_node_cores: 2,
            etcd_timeout_rate: 0.0,
        }
    }
}

/// A risingwave cluster.
///
/// # Nodes
///
/// | Name           | IP            |
/// | -------------- | ------------- |
/// | meta           | 192.168.1.1   |
/// | frontend-x     | 192.168.2.x   |
/// | compute-x      | 192.168.3.x   |
/// | compactor-x    | 192.168.4.x   |
/// | etcd           | 192.168.10.1  |
/// | kafka-broker   | 192.168.11.1  |
/// | kafka-producer | 192.168.11.2  |
/// | client         | 192.168.100.1 |
/// | ctl            | 192.168.101.1 |
pub struct Cluster {
    config: Configuration,
    handle: Handle,
    pub(crate) client: NodeHandle,
    pub(crate) ctl: NodeHandle,
}

impl Cluster {
    pub fn start(conf: Configuration) -> BoxFuture<'static, Result<Self>> {
        Box::pin(Self::start_inner(conf))
    }

    async fn start_inner(conf: Configuration) -> Result<Self> {
        let handle = madsim::runtime::Handle::current();
        println!("seed = {}", handle.seed());
        println!("{:?}", conf);
        println!("config path = {}", CONFIG_PATH.display());

        // etcd node
        handle
            .create_node()
            .name("etcd")
            .ip("192.168.10.1".parse().unwrap())
            .init(move || async move {
                let addr = "0.0.0.0:2388".parse().unwrap();
                etcd_client::SimServer::builder()
                    .timeout_rate(conf.etcd_timeout_rate)
                    .serve(addr)
                    .await
                    .unwrap();
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

        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let meta = "192.168.1.1".parse().unwrap();
        std::env::set_var("RW_META_ADDR", format!("https://{meta}:5690/"));

        // meta node
        handle
            .create_node()
            .name("meta")
            .ip(meta)
            .init(|| async {
                let opts = risingwave_meta::MetaNodeOpts::parse_from([
                    "meta-node",
                    "--config-path",
                    &CONFIG_PATH.as_os_str().to_string_lossy(),
                    "--listen-addr",
                    "0.0.0.0:5690",
                    "--backend",
                    "etcd",
                    "--etcd-endpoints",
                    "192.168.10.1:2388",
                ]);
                risingwave_meta::start(opts).await
            })
            .build();
        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        // frontend node
        for i in 1..=conf.frontend_nodes {
            handle
                .create_node()
                .name(format!("frontend-{i}"))
                .ip([192, 168, 2, i as u8].into())
                .init(move || async move {
                    let opts = risingwave_frontend::FrontendOpts::parse_from([
                        "frontend-node",
                        "--config-path",
                        &CONFIG_PATH.as_os_str().to_string_lossy(),
                        "--host",
                        "0.0.0.0:4566",
                        "--client-address",
                        &format!("192.168.2.{i}:4566"),
                        "--meta-addr",
                        &format!("{meta}:5690"),
                    ]);
                    risingwave_frontend::start(opts).await
                })
                .build();
        }

        // compute node
        for i in 1..=conf.compute_nodes {
            handle
                .create_node()
                .name(format!("compute-{i}"))
                .ip([192, 168, 3, i as u8].into())
                .cores(conf.compute_node_cores)
                .init(move || async move {
                    let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                        "compute-node",
                        "--config-path",
                        &CONFIG_PATH.as_os_str().to_string_lossy(),
                        "--host",
                        "0.0.0.0:5688",
                        "--client-address",
                        &format!("192.168.3.{i}:5688"),
                        "--meta-address",
                        &format!("{meta}:5690"),
                        "--state-store",
                        "hummock+memory-shared",
                    ]);
                    risingwave_compute::start(opts).await
                })
                .build();
        }

        // compactor node
        for i in 1..=conf.compactor_nodes {
            handle
                .create_node()
                .name(format!("compactor-{i}"))
                .ip([192, 168, 4, i as u8].into())
                .init(move || async move {
                    let opts = risingwave_compactor::CompactorOpts::parse_from([
                        "compactor-node",
                        "--config-path",
                        &CONFIG_PATH.as_os_str().to_string_lossy(),
                        "--host",
                        "0.0.0.0:6660",
                        "--client-address",
                        &format!("192.168.4.{i}:6660"),
                        "--meta-address",
                        &format!("{meta}:5690"),
                        "--state-store",
                        "hummock+memory-shared",
                    ]);
                    risingwave_compactor::start(opts).await
                })
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

    /// Run a SQL query from the client.
    pub fn run(&mut self, sql: &str) -> BoxFuture<'_, Result<String>> {
        Box::pin(self.run_inner(sql.to_string()))
    }

    async fn run_inner(&mut self, sql: String) -> Result<String> {
        let frontend = self.rand_frontend_ip();

        let result = self
            .client
            .spawn(async move {
                // TODO: reuse session
                let mut session = RisingWave::connect(frontend, "dev".to_string())
                    .await
                    .expect("failed to connect to RisingWave");
                let result = session.run(&sql).await?;
                Ok::<_, anyhow::Error>(result)
            })
            .await??;

        Ok(result)
    }

    /// Run a future on the client node.
    pub async fn run_on_client<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.client.spawn(future).await.unwrap();
    }

    /// Run a SQL query from the client and wait until the condition is met.
    pub fn wait_until(
        &mut self,
        sql: &str,
        p: impl FnMut(&str) -> bool + Send + 'static,
        interval: Duration,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(self.wait_until_inner(sql.to_string(), p, interval, timeout))
    }

    async fn wait_until_inner(
        &mut self,
        sql: String,
        mut p: impl FnMut(&str) -> bool + Send + 'static,
        interval: Duration,
        timeout: Duration,
    ) -> Result<String> {
        let fut = async move {
            let mut interval = madsim::time::interval(interval);
            loop {
                interval.tick().await;
                let result = self.run(&sql).await?;
                if p(&result) {
                    return Ok::<_, anyhow::Error>(result);
                }
            }
        };

        match madsim::time::timeout(timeout, fut).await {
            Ok(r) => Ok(r?),
            Err(_) => bail!("wait_until timeout"),
        }
    }

    async fn wait_until_non_empty_inner(
        &mut self,
        sql: String,
        interval: Duration,
        timeout: Duration,
    ) -> Result<String> {
        self.wait_until_inner(sql, |r| !r.trim().is_empty(), interval, timeout)
            .await
    }

    /// Run a SQL query from the client and wait until the return result is not empty.
    pub fn wait_until_non_empty(
        &mut self,
        sql: &str,
        interval: Duration,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(self.wait_until_non_empty_inner(sql.to_string(), interval, timeout))
    }

    /// Kill some nodes and restart them in 2s.
    pub async fn kill_node(&self, opts: &KillOpts) {
        let mut nodes = vec![];
        if opts.kill_meta {
            nodes.push(format!("meta"));
        }
        if opts.kill_frontend {
            let i = rand::thread_rng().gen_range(1..=self.config.frontend_nodes);
            nodes.push(format!("frontend-{}", i));
        }
        if opts.kill_compute {
            let i = rand::thread_rng().gen_range(1..=self.config.compute_nodes);
            nodes.push(format!("compute-{}", i));
        }
        if opts.kill_compactor {
            let i = rand::thread_rng().gen_range(1..=self.config.compactor_nodes);
            nodes.push(format!("compactor-{}", i));
        }
        if nodes.is_empty() {
            return;
        }
        join_all(nodes.iter().map(|name| async move {
            let t = rand::thread_rng().gen_range(Duration::from_secs(0)..Duration::from_secs(1));
            tokio::time::sleep(t).await;
            tracing::info!("kill {name}");
            madsim::runtime::Handle::current().kill(&name);

            let t = rand::thread_rng().gen_range(Duration::from_secs(0)..Duration::from_secs(1));
            tokio::time::sleep(t).await;
            tracing::info!("restart {name}");
            madsim::runtime::Handle::current().restart(&name);
        }))
        .await;
    }

    /// Create a node for kafka producer and prepare data.
    pub fn create_kafka_producer(&self, datadir: &str) {
        self.handle
            .create_node()
            .name("kafka-producer")
            .ip("192.168.11.2".parse().unwrap())
            .build()
            .spawn(crate::kafka::producer(
                "192.168.11.1:29092",
                datadir.to_string(),
            ));
    }

    /// Return the IP of a random frontend node.
    pub fn rand_frontend_ip(&self) -> String {
        let i = rand::thread_rng().gen_range(1..=self.config.frontend_nodes);
        format!("192.168.2.{i}")
    }

    /// Return the IP of all frontend nodes.
    pub fn frontend_ips(&self) -> Vec<String> {
        (1..=self.config.frontend_nodes)
            .map(|i| format!("192.168.2.{i}"))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KillOpts {
    pub kill_rate: f32,
    pub kill_meta: bool,
    pub kill_frontend: bool,
    pub kill_compute: bool,
    pub kill_compactor: bool,
}
