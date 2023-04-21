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

#![cfg_attr(not(madsim), allow(dead_code))]
#![feature(lazy_cell)]

use std::path::PathBuf;

use clap::Parser;

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
    #[clap(default_value = "")]
    files: String,

    /// The number of frontend nodes.
    #[clap(long, default_value = "2")]
    frontend_nodes: usize,

    /// The number of compute nodes.
    #[clap(long, default_value = "3")]
    compute_nodes: usize,

    /// The number of compactor nodes.
    #[clap(long, default_value = "2")]
    compactor_nodes: usize,

    /// The number of meta nodes.
    #[clap(long, default_value = "3")]
    meta_nodes: usize,

    /// The number of CPU cores for each compute node.
    ///
    /// This determines worker_node_parallelism.
    #[clap(long, default_value = "2")]
    compute_node_cores: usize,

    /// The number of clients to run simultaneously.
    ///
    /// If this argument is set, the runner will implicitly create a database for each test file,
    /// and all `--kill*` options will be ignored.
    #[clap(short, long)]
    jobs: Option<usize>,

    /// The probability of etcd request timeout.
    #[clap(long, default_value = "0.0")]
    etcd_timeout_rate: f32,

    /// Allow to kill all risingwave node.
    #[clap(long)]
    kill: bool,

    /// Allow to kill meta node.
    #[clap(long)]
    kill_meta: bool,

    /// Allow to kill frontend node.
    #[clap(long)]
    kill_frontend: bool,

    /// Allow to kill compute node.
    #[clap(long)]
    kill_compute: bool,

    /// Allow to kill compactor node.
    #[clap(long)]
    kill_compactor: bool,

    /// The probability of a node being killed.
    #[clap(long, default_value = "1.0")]
    kill_rate: f32,

    /// Delay in restart after kill.
    #[clap(long, default_value = "30")]
    restart_delay_secs: u32,

    /// The directory of kafka source data.
    #[clap(long)]
    kafka_datadir: Option<String>,

    /// Path to configuration file.
    #[clap(long)]
    config_path: Option<String>,

    /// The number of sqlsmith test cases to generate.
    ///
    /// If this argument is set, the `files` argument refers to a directory containing sqlsmith
    /// test data.
    #[clap(long)]
    sqlsmith: Option<usize>,

    /// Run sqlsmith pre-generated queries with the given [`files`] directory,
    /// containing `ddl.sql` and `queries.sql`.
    #[clap(long)]
    run_sqlsmith_queries: bool,

    /// Run sqlsmith to generate queries with the given testdata [`files`],
    /// and output the ddl + queries to the given directory,
    /// indicated by this argument.
    /// We generate sqlsmith queries via `madsim` because
    /// it provides a degree of determinism, and we can spawn several
    /// instances in parallel.
    #[clap(long)]
    generate_sqlsmith_queries: Option<String>,

    /// Load etcd data from toml file.
    #[clap(long)]
    etcd_data: Option<PathBuf>,

    /// Dump etcd data into toml file before exit.
    #[clap(long)]
    etcd_dump: Option<PathBuf>,

    #[arg(short, long)]
    e2e_extended_test: bool,
}

#[cfg(madsim)]
#[madsim::main]
async fn main() {
    use std::sync::Arc;

    use risingwave_simulation::client::RisingWave;
    use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, KillOpts};
    use risingwave_simulation::slt::*;
    use tracing_subscriber::EnvFilter;

    _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        // no ANSI color codes when output to file
        .with_ansi(console::colors_enabled_stderr() && console::colors_enabled())
        .with_writer(std::io::stderr)
        .try_init();

    let args = Args::parse();
    let config = Configuration {
        config_path: ConfigPath::Regular(args.config_path.unwrap_or_default()),
        frontend_nodes: args.frontend_nodes,
        compute_nodes: args.compute_nodes,
        compactor_nodes: args.compactor_nodes,
        compute_node_cores: args.compute_node_cores,
        meta_nodes: args.meta_nodes,
        etcd_timeout_rate: args.etcd_timeout_rate,
        etcd_data_path: args.etcd_data,
    };
    let kill_opts = KillOpts {
        kill_meta: args.kill_meta || args.kill,
        kill_frontend: args.kill_frontend || args.kill,
        kill_compute: args.kill_compute || args.kill,
        kill_compactor: args.kill_compactor || args.kill,
        kill_rate: args.kill_rate,
        restart_delay_secs: args.restart_delay_secs,
    };

    let cluster = Arc::new(
        Cluster::start(config)
            .await
            .expect("failed to start cluster"),
    );

    if let Some(datadir) = args.kafka_datadir {
        cluster.create_kafka_producer(&datadir).await;
    }

    let seed = madsim::runtime::Handle::current().seed();
    if let Some(count) = args.sqlsmith {
        cluster
            .run_on_client(async move {
                let rw = RisingWave::connect("frontend".into(), "dev".into())
                    .await
                    .unwrap();
                if let Some(outdir) = args.generate_sqlsmith_queries {
                    risingwave_sqlsmith::runner::generate(
                        rw.pg_client(),
                        &args.files,
                        count,
                        &outdir,
                        Some(seed),
                    )
                    .await;
                } else {
                    risingwave_sqlsmith::runner::run(
                        rw.pg_client(),
                        &args.files,
                        count,
                        Some(seed),
                    )
                    .await;
                }
            })
            .await;
        return;
    }

    if args.run_sqlsmith_queries {
        let outdir = args.files;
        cluster
            .run_on_client(async move {
                let rw = RisingWave::connect("frontend".into(), "dev".into())
                    .await
                    .unwrap();
                risingwave_sqlsmith::runner::run_pre_generated(rw.pg_client(), &outdir).await;
            })
            .await;
        return;
    }

    let cluster0 = cluster.clone();
    cluster
        .run_on_client(async move {
            let glob = &args.files;
            if let Some(jobs) = args.jobs {
                run_parallel_slt_task(glob, jobs).await.unwrap();
            } else {
                run_slt_task(cluster0, glob, &kill_opts).await;
            }
        })
        .await;

    if let Some(path) = args.etcd_dump {
        cluster
            .run_on_client(async move {
                let mut client = etcd_client::Client::connect(["192.168.10.1:2388"], None)
                    .await
                    .unwrap();
                let dump = client.dump().await.unwrap();
                std::fs::write(path, dump).unwrap();
            })
            .await;
    }

    if args.e2e_extended_test {
        cluster
            .run_on_client(async move {
                risingwave_e2e_extended_mode_test::run_test_suit(
                    "dev".to_string(),
                    "root".to_string(),
                    "frontend".to_string(),
                    4566,
                    "".to_string(),
                )
                .await;
            })
            .await;
    }

    cluster.graceful_shutdown().await;
}
