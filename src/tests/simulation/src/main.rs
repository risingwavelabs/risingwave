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

use std::time::Duration;

use clap::Parser;

#[cfg(not(madsim))]
fn main() {
    println!("This binary is only available in simulation.");
}

/// Determinisitic simulation end-to-end test runner.
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
    #[clap(long, default_value = "1")]
    frontend_nodes: usize,

    /// The number of compute nodes.
    #[clap(long, default_value = "3")]
    compute_nodes: usize,

    /// The number of CPU cores for each compute node.
    ///
    /// This determines worker_node_parallelism.
    #[clap(long, default_value = "2")]
    compute_node_cores: usize,

    #[clap(short, long, default_value = "1")]
    jobs: usize,
}

#[cfg(madsim)]
#[madsim::main]
async fn main() {
    let args = Args::parse();

    let handle = madsim::runtime::Handle::current();
    println!("seed = {}", handle.seed());
    println!("{:?}", args);

    // meta node
    handle
        .create_node()
        .name("meta")
        .ip("192.168.1.1".parse().unwrap())
        .init(|| async {
            let opts = risingwave_meta::MetaNodeOpts::parse_from([
                "meta-node",
                "--listen-addr",
                "0.0.0.0:5690",
            ]);
            risingwave_meta::start(opts).await
        })
        .build();
    // wait for the service to be ready
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

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
        handle
            .create_node()
            .name(format!("compute-{i}"))
            .ip([192, 168, 3, i as u8].into())
            .cores(args.compute_node_cores)
            .init(move || async move {
                let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                    "compute-node",
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
    client_node
        .spawn(async move { run_slt_task(&args.files, &args, &frontend_ip).await })
        .await
        .unwrap();
}

async fn run_slt_task(glob: &str, args: &Args, hosts: &[String]) {
    let db = Postgres::connect(hosts[0].clone(), "dev".to_string()).await;
    let mut tester = sqllogictest::ParallelRunner::new(db, hosts.to_vec());
    let tasks = tester
        .prepare_async(glob, Postgres::connect)
        .await
        .expect("prepare failed");
    tester
        .run_parallel_async(tasks, args.jobs)
        .await
        .expect("test failed");
}

struct Postgres {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Postgres {
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
        Postgres { client, task }
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Postgres {
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
