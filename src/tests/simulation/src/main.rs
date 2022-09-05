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

#![cfg_attr(not(madsim), allow(dead_code))]
#![feature(once_cell)]

use std::sync::LazyLock;
use std::time::Duration;

use clap::Parser;
use rand::Rng;
use sqllogictest::ParallelTestError;

#[cfg(not(madsim))]
fn main() {
    println!("This binary is only available in simulation.");
}

/// Deterministic simulation end-to-end test runner.
///
/// ENVS:
///
///     RUST_LOG            Set the log level.
///
///     MADSIM_TEST_SEED    Random seed for this run.
///
///     MADSIM_TEST_NUM     The number of runs.
#[derive(Debug, Parser)]
pub struct Args {
    /// Glob of sqllogictest scripts.
    #[clap()]
    files: String,

    /// The number of frontend nodes.
    #[clap(long, default_value = "2")]
    frontend_nodes: usize,

    /// The number of compute nodes.
    #[clap(long, default_value = "3")]
    compute_nodes: usize,

    /// The number of compactor nodes.
    #[clap(long, default_value = "1")]
    compactor_nodes: usize,

    /// The number of CPU cores for each compute node.
    ///
    /// This determines worker_node_parallelism.
    #[clap(long, default_value = "2")]
    compute_node_cores: usize,

    /// The number of clients to run simultaneously.
    ///
    /// If this argument is set, the runner will implicitly create a database for each test file.
    #[clap(short, long)]
    jobs: Option<usize>,

    /// Randomly kill the meta node after each query.
    ///
    /// Currently only available when `-j` is not set.
    #[clap(long)]
    kill_meta: bool,

    /// Randomly kill a frontend node after each query.
    ///
    /// Currently only available when `-j` is not set.
    #[clap(long)]
    kill_frontend: bool,

    /// Randomly kill a compute node after each query.
    ///
    /// Currently only available when `-j` is not set.
    #[clap(long)]
    kill_compute: bool,

    /// Randomly kill a compactor node after each query.
    ///
    /// Currently only available when `-j` is not set.
    #[clap(long)]
    kill_compactor: bool,

    /// The number of sqlsmith test cases to generate.
    ///
    /// If this argument is set, the `files` argument refers to a directory containing sqlsmith
    /// test data.
    #[clap(long)]
    sqlsmith: Option<usize>,
}

static ARGS: LazyLock<Args> = LazyLock::new(Args::parse);

#[cfg(madsim)]
#[madsim::main]
async fn main() {
    let args = &*ARGS;

    let handle = madsim::runtime::Handle::current();
    println!("seed = {}", handle.seed());
    println!("{:?}", args);

    // etcd node
    handle
        .create_node()
        .name("etcd")
        .ip("192.168.10.1".parse().unwrap())
        .init(|| async {
            let addr = "0.0.0.0:2388".parse().unwrap();
            etcd_client::SimServer::builder()
                .timeout_rate(0.01)
                .serve(addr)
                .await
                .unwrap();
        })
        .build();
    // wait for the service to be ready
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // meta node
    handle
        .create_node()
        .name("meta")
        .ip("192.168.1.1".parse().unwrap())
        .init(|| async {
            let opts = risingwave_meta::MetaNodeOpts::parse_from([
                "meta-node",
                // "--config-path",
                // "src/config/risingwave.toml",
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
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // frontend node
    let mut frontend_ip = vec![];
    for i in 1..=args.frontend_nodes {
        frontend_ip.push(format!("192.168.2.{i}"));
        handle
            .create_node()
            .name(format!("frontend-{i}"))
            .ip([192, 168, 2, i as u8].into())
            .init(move || async move {
                let opts = risingwave_frontend::FrontendOpts::parse_from([
                    "frontend-node",
                    "--host",
                    "0.0.0.0:4566",
                    "--client-address",
                    &format!("192.168.2.{i}:4566"),
                    "--meta-addr",
                    "192.168.1.1:5690",
                ]);
                risingwave_frontend::start(opts).await
            })
            .build();
    }

    // compute node
    for i in 1..=args.compute_nodes {
        let mut builder = handle
            .create_node()
            .name(format!("compute-{i}"))
            .ip([192, 168, 3, i as u8].into())
            .cores(args.compute_node_cores)
            .init(move || async move {
                let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                    "compute-node",
                    // "--config-path",
                    // "src/config/risingwave.toml",
                    "--host",
                    "0.0.0.0:5688",
                    "--client-address",
                    &format!("192.168.3.{i}:5688"),
                    "--meta-address",
                    "192.168.1.1:5690",
                    "--state-store",
                    "hummock+memory-shared",
                ]);
                risingwave_compute::start(opts).await
            });
        if args.kill_compute || args.kill_meta {
            builder = builder.restart_on_panic();
        }
        builder.build();
    }

    // compactor node
    for i in 1..=args.compactor_nodes {
        handle
            .create_node()
            .name(format!("compactor-{i}"))
            .ip([192, 168, 4, i as u8].into())
            .init(move || async move {
                let opts = risingwave_compactor::CompactorOpts::parse_from([
                    "compactor-node",
                    // "--config-path",
                    // "src/config/risingwave.toml",
                    "--host",
                    "0.0.0.0:6660",
                    "--client-address",
                    &format!("192.168.4.{i}:6660"),
                    "--meta-address",
                    "192.168.1.1:5690",
                    "--state-store",
                    "hummock+memory-shared",
                ]);
                risingwave_compactor::start(opts).await
            })
            .build();
    }

    // wait for the service to be ready
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // client
    let client_node = handle
        .create_node()
        .name("client")
        .ip([192, 168, 100, 1].into())
        .build();

    if let Some(count) = args.sqlsmith {
        client_node
            .spawn(async move {
                let i = rand::thread_rng().gen_range(0..frontend_ip.len());
                let host = frontend_ip[i].clone();
                let rw = Risingwave::connect(host, "dev".into()).await;
                risingwave_sqlsmith::runner::run(&rw.client, &args.files, count).await;
            })
            .await
            .unwrap();
        return;
    }

    client_node
        .spawn(async move {
            let glob = &args.files;
            if let Some(jobs) = args.jobs {
                run_parallel_slt_task(glob, &frontend_ip, jobs)
                    .await
                    .unwrap();
            } else {
                let i = rand::thread_rng().gen_range(0..frontend_ip.len());
                run_slt_task(glob, &frontend_ip[i]).await;
            }
        })
        .await
        .unwrap();
}

#[cfg(madsim)]
async fn kill_node() {
    let mut nodes = vec![];
    if ARGS.kill_meta {
        nodes.push(format!("meta"));
    }
    if ARGS.kill_frontend {
        // FIXME: handle postgres connection error
        let i = rand::thread_rng().gen_range(1..=ARGS.frontend_nodes);
        nodes.push(format!("frontend-{}", i));
    }
    if ARGS.kill_compute {
        let i = rand::thread_rng().gen_range(1..=ARGS.compute_nodes);
        nodes.push(format!("compute-{}", i));
    }
    if ARGS.kill_compactor {
        let i = rand::thread_rng().gen_range(1..=ARGS.compactor_nodes);
        nodes.push(format!("compactor-{}", i));
    }
    if nodes.is_empty() {
        return;
    }
    for name in &nodes {
        tracing::info!("kill {name}");
        madsim::runtime::Handle::current().kill(&name);
    }
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    for name in &nodes {
        tracing::info!("restart {name}");
        madsim::runtime::Handle::current().restart(&name);
    }
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
}

#[cfg(not(madsim))]
async fn kill_node() {}

struct HookImpl;

#[async_trait::async_trait]
impl sqllogictest::Hook for HookImpl {
    async fn on_stmt_complete(&mut self, _sql: &str) {
        kill_node().await;
    }

    async fn on_query_complete(&mut self, _sql: &str) {
        kill_node().await;
    }
}

async fn run_slt_task(glob: &str, host: &str) {
    let risingwave = Risingwave::connect(host.to_string(), "dev".to_string()).await;
    if ARGS.kill_compute {
        risingwave
            .client
            .simple_query("SET RW_IMPLICIT_FLUSH TO true;")
            .await
            .expect("failed to set");
    }
    let mut tester = sqllogictest::Runner::new(risingwave);
    tester.set_hook(HookImpl);
    let files = glob::glob(glob).expect("failed to read glob pattern");
    for file in files {
        let file = file.unwrap();
        let path = file.as_path();
        println!("{}", path.display());
        tester.run_file_async(path).await.unwrap();
    }
}

async fn run_parallel_slt_task(
    glob: &str,
    hosts: &[String],
    jobs: usize,
) -> Result<(), ParallelTestError> {
    let i = rand::thread_rng().gen_range(0..hosts.len());
    let db = Risingwave::connect(hosts[i].clone(), "dev".to_string()).await;
    let mut tester = sqllogictest::Runner::new(db);
    tester
        .run_parallel_async(glob, hosts.to_vec(), Risingwave::connect, jobs)
        .await
}

struct Risingwave {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Risingwave {
    async fn connect(host: String, dbname: String) -> Self {
        let (client, connection) = tokio_postgres::Config::new()
            .host(&host)
            .port(4566)
            .dbname(&dbname)
            .user("root")
            .connect_timeout(Duration::from_secs(5))
            .connect(tokio_postgres::NoTls)
            .await
            .expect("Failed to connect to database");
        let task = tokio::spawn(async move {
            connection.await.expect("Postgres connection error");
        });
        Risingwave { client, task }
    }
}

impl Drop for Risingwave {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Risingwave {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        use std::fmt::Write;

        let mut output = String::new();
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) if v.is_empty() => write!(output, "(empty)").unwrap(),
                            Some(v) => write!(output, "{}", v).unwrap(),
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }

    fn engine_name(&self) -> &str {
        "risingwave"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
