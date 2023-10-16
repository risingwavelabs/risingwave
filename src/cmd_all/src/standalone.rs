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
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta::MetaNodeOpts;
use shell_words::split;
use tokio::signal;

use crate::common::osstrs;

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
pub struct StandaloneOpts {
    /// Compute node options
    /// If missing, compute node won't start
    #[clap(short, long, env = "STANDALONE_COMPUTE_OPTS")]
    compute_opts: Option<String>,

    #[clap(short, long, env = "STANDALONE_META_OPTS")]
    /// Meta node options
    /// If missing, meta node won't start
    meta_opts: Option<String>,

    #[clap(short, long, env = "STANDALONE_FRONTEND_OPTS")]
    /// Frontend node options
    /// If missing, frontend node won't start
    frontend_opts: Option<String>,

    #[clap(long, env = "STANDALONE_COMPACTOR_OPTS")]
    /// Compactor node options
    /// If missing compactor node won't start
    compactor_opts: Option<String>,

    #[clap(long, env = "PROMETHEUS_LISTENER_ADDR")]
    /// Prometheus listener address
    /// If present, it will override prometheus listener address for
    /// Frontend, Compute and Compactor nodes
    prometheus_listener_addr: Option<String>,
}

#[derive(Debug)]
pub struct ParsedStandaloneOpts {
    pub meta_opts: Option<MetaNodeOpts>,
    pub compute_opts: Option<ComputeNodeOpts>,
    pub frontend_opts: Option<FrontendOpts>,
    pub compactor_opts: Option<CompactorOpts>,
}

fn parse_opt_args(opts: &StandaloneOpts) -> ParsedStandaloneOpts {
    let meta_opts = opts.meta_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "meta-node".into());
        s
    });
    let meta_opts = meta_opts.map(|o| MetaNodeOpts::parse_from(osstrs(o)));
    let compute_opts = opts.compute_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "compute-node".into());
        s
    });
    let compute_opts = compute_opts.map(|o| ComputeNodeOpts::parse_from(osstrs(o)));
    let frontend_opts = opts.frontend_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "frontend-node".into());
        s
    });
    let frontend_opts = frontend_opts.map(|o| FrontendOpts::parse_from(osstrs(o)));
    let compactor_opts = opts.compactor_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "compactor-node".into());
        s
    });
    let compactor_opts = compactor_opts.map(|o| CompactorOpts::parse_from(osstrs(o)));
    ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    }
}

pub async fn standalone(opts: StandaloneOpts) -> Result<()> {
    tracing::info!("launching Risingwave in standalone mode");

    let ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    } = parse_opt_args(&opts);

    if let Some(opts) = meta_opts {
        tracing::info!("starting meta-node thread with cli args: {:?}", opts);
        let _meta_handle = tokio::spawn(async move {
            risingwave_meta::start(opts).await;
            tracing::warn!("meta is stopped, shutdown all nodes");
        });
        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    if let Some(opts) = compute_opts {
        tracing::info!("starting compute-node thread with cli args: {:?}", opts);
        let _compute_handle = tokio::spawn(async move { risingwave_compute::start(opts).await });
    }
    if let Some(opts) = frontend_opts {
        tracing::info!("starting frontend-node thread with cli args: {:?}", opts);
        let _frontend_handle = tokio::spawn(async move { risingwave_frontend::start(opts).await });
    }
    if let Some(opts) = compactor_opts {
        tracing::info!("starting compactor-node thread with cli args: {:?}", opts);
        let _compactor_handle =
            tokio::spawn(async move { risingwave_compactor::start(opts).await });
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
            state_store_url: None,
            prometheus_listener_addr: None,
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
