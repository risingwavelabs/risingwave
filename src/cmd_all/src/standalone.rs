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

use anyhow::Result;
use clap::Parser;
use shell_words::split;
use tokio::signal;

use crate::common::{osstrs, RisingWaveService};

#[derive(Debug, Clone, Parser)]
pub struct StandaloneOpts {
    /// Compute node options
    #[clap(short, long, env = "STANDALONE_COMPUTE_OPTS", default_value = "")]
    compute_opts: String,

    #[clap(short, long, env = "STANDALONE_META_OPTS", default_value = "")]
    /// Meta node options
    meta_opts: String,

    #[clap(short, long, env = "STANDALONE_FRONTEND_OPTS", default_value = "")]
    /// Frontend node options
    frontend_opts: String,
}

fn parse_opt_args(opts: &StandaloneOpts) -> (Vec<String>, Vec<String>, Vec<String>) {
    let meta_opts = split(&opts.meta_opts).unwrap();
    let compute_opts = split(&opts.compute_opts).unwrap();
    let frontend_opts = split(&opts.frontend_opts).unwrap();
    (meta_opts, compute_opts, frontend_opts)
}

fn get_services(opts: &StandaloneOpts) -> Vec<RisingWaveService> {
    let (meta_opts, compute_opts, frontend_opts) = parse_opt_args(opts);
    let services = vec![
        RisingWaveService::Meta(osstrs(meta_opts)),
        RisingWaveService::Compute(osstrs(compute_opts)),
        RisingWaveService::Frontend(osstrs(frontend_opts)),
    ];
    services
}

pub async fn standalone(opts: StandaloneOpts) -> Result<()> {
    tracing::info!("launching Risingwave in standalone mode");

    let services = get_services(&opts);

    for service in services {
        match service {
            RisingWaveService::Meta(mut opts) => {
                opts.insert(0, "meta-node".into());
                tracing::info!("starting meta-node thread with cli args: {:?}", opts);
                let opts = risingwave_meta::MetaNodeOpts::parse_from(opts);
                let _meta_handle = tokio::spawn(async move {
                    risingwave_meta::start(opts).await;
                    tracing::warn!("meta is stopped, shutdown all nodes");
                });
                // wait for the service to be ready
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            RisingWaveService::Compute(mut opts) => {
                opts.insert(0, "compute-node".into());
                tracing::info!("starting compute-node thread with cli args: {:?}", opts);
                let opts = risingwave_compute::ComputeNodeOpts::parse_from(opts);
                let _compute_handle =
                    tokio::spawn(async move { risingwave_compute::start(opts).await });
            }
            RisingWaveService::Frontend(mut opts) => {
                opts.insert(0, "frontend-node".into());
                tracing::info!("starting frontend-node thread with cli args: {:?}", opts);
                let opts = risingwave_frontend::FrontendOpts::parse_from(opts);
                let _frontend_handle =
                    tokio::spawn(async move { risingwave_frontend::start(opts).await });
            }
            RisingWaveService::Compactor(_) => {
                panic!("Compactor node unsupported in Risingwave standalone mode.");
            }
            RisingWaveService::ConnectorNode(_) => {
                panic!("Connector node unsupported in Risingwave standalone mode.");
            }
        }
    }

    // wait for log messages to be flushed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    eprintln!("-------------------------------");
    eprintln!("RisingWave standalone mode is ready.");

    // TODO: should we join all handles?
    // Currently, not all services can be shutdown gracefully, just quit on Ctrl-C now.
    // TODO(kwannoel): Why can't be shutdown gracefully? Is it that the service just does not
    // support it?
    signal::ctrl_c().await.unwrap();
    tracing::info!("Ctrl+C received, now exiting");

    Ok(())
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_parse_opt_args() {
        let opts = StandaloneOpts {
            compute_opts: "--listen-address 127.0.0.1 --port 8000".into(),
            meta_opts: "--data-dir \"some path with spaces\" --port 8001".into(),
            frontend_opts: "--some-option".into(),
        };
        let actual = parse_opt_args(&opts);
        check(
            actual,
            expect![[r#"
            (
                [
                    "--data-dir",
                    "some path with spaces",
                    "--port",
                    "8001",
                ],
                [
                    "--listen-address",
                    "127.0.0.1",
                    "--port",
                    "8000",
                ],
                [
                    "--some-option",
                ],
            )"#]],
        );
    }
}
