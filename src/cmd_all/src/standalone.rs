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

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
pub struct StandaloneOpts {
    /// Compute node options
    #[clap(short, long, env = "STANDALONE_COMPUTE_OPTS")]
    compute_opts: Option<String>,

    #[clap(short, long, env = "STANDALONE_META_OPTS")]
    /// Meta node options
    meta_opts: Option<String>,

    #[clap(short, long, env = "STANDALONE_FRONTEND_OPTS")]
    /// Frontend node options
    frontend_opts: Option<String>,

    #[clap(long, env = "STANDALONE_COMPACTOR_OPTS")]
    /// Frontend node options
    compactor_opts: Option<String>,
}

#[derive(Debug)]
pub struct ParsedStandaloneOpts {
    pub meta_opts: Option<Vec<String>>,
    pub compute_opts: Option<Vec<String>>,
    pub frontend_opts: Option<Vec<String>>,
    pub compactor_opts: Option<Vec<String>>,
}

fn parse_opt_args(opts: &StandaloneOpts) -> ParsedStandaloneOpts {
    let meta_opts = opts.meta_opts.as_ref().map(|s| split(s).unwrap());
    let compute_opts = opts.compute_opts.as_ref().map(|s| split(s).unwrap());
    let frontend_opts = opts.frontend_opts.as_ref().map(|s| split(s).unwrap());
    let compactor_opts = opts.compactor_opts.as_ref().map(|s| split(s).unwrap());
    ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    }
}

fn get_services(opts: &StandaloneOpts) -> Vec<RisingWaveService> {
    let ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    } = parse_opt_args(opts);
    let mut services = vec![];
    if let Some(meta_opts) = meta_opts {
        services.push(RisingWaveService::Meta(osstrs(meta_opts)));
    }
    if let Some(compute_opts) = compute_opts {
        services.push(RisingWaveService::Compute(osstrs(compute_opts)));
    }
    if let Some(frontend_opts) = frontend_opts {
        services.push(RisingWaveService::Frontend(osstrs(frontend_opts)));
    }
    if let Some(compactor_opts) = compactor_opts {
        services.push(RisingWaveService::Compactor(osstrs(compactor_opts)));
    }
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
            RisingWaveService::Compactor(mut opts) => {
                opts.insert(0, "compactor-node".into());
                tracing::info!("starting compactor-node thread with cli args: {:?}", opts);
                let opts = risingwave_compactor::CompactorOpts::parse_from(opts);
                let _compactor_handle =
                    tokio::spawn(async move { risingwave_compactor::start(opts).await });
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
        // Test parsing into standalone-level opts.
        let raw_opts = "
--compute-opts=--listen-address 127.0.0.1 --port 8000
--meta-opts=--data-dir \"some path with spaces\" --port 8001
--frontend-opts=--some-option
";
        let actual = StandaloneOpts::parse_from(raw_opts.lines());
        let opts = StandaloneOpts {
            compute_opts: Some("--listen-address 127.0.0.1 --port 8000".into()),
            meta_opts: Some("--data-dir \"some path with spaces\" --port 8001".into()),
            frontend_opts: Some("--some-option".into()),
            compactor_opts: None,
        };
        assert_eq!(actual, opts);

        // Test parsing into node-level opts.
        let actual = parse_opt_args(&opts);
        check(
            actual,
            expect![[r#"
                ParsedStandaloneOpts {
                    meta_opts: Some(
                        [
                            "--data-dir",
                            "some path with spaces",
                            "--port",
                            "8001",
                        ],
                    ),
                    compute_opts: Some(
                        [
                            "--listen-address",
                            "127.0.0.1",
                            "--port",
                            "8000",
                        ],
                    ),
                    frontend_opts: Some(
                        [
                            "--some-option",
                        ],
                    ),
                    compactor_opts: None,
                }"#]],
        );
    }
}
