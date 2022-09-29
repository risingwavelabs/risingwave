use std::net::IpAddr;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use madsim::rand::thread_rng;
use madsim::runtime::{Handle, NodeHandle};
use rand::seq::SliceRandom;
use sqllogictest::AsyncDB;

use crate::{Args, Risingwave};

pub struct Cluster {
    meta: IpAddr,
    frontends: Vec<IpAddr>,

    handle: Handle,

    client: NodeHandle,
}

impl Cluster {
    pub async fn start() -> Result<Self> {
        let args = Args::parse_from::<_, &str>([]);

        let handle = madsim::runtime::Handle::current();
        println!("seed = {}", handle.seed());
        println!("{:?}", args);

        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let meta = "192.168.1.1".parse().unwrap();

        // meta node
        handle
            .create_node()
            .name("meta")
            .ip(meta)
            .init(|| async {
                let opts = risingwave_meta::MetaNodeOpts::parse_from([
                    "meta-node",
                    // "--config-path",
                    // "src/config/risingwave.toml",
                    "--listen-addr",
                    "0.0.0.0:5690",
                    "--backend",
                    "mem",
                ]);
                risingwave_meta::start(opts).await
            })
            .build();
        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        // frontend node
        let mut frontends = vec![];
        for i in 1..=args.frontend_nodes {
            let frontend_ip = format!("192.168.2.{i}").parse().unwrap();
            frontends.push(frontend_ip);
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
                        &format!("{frontend_ip}:4566"),
                        "--meta-addr",
                        &format!("192.168.1.1:5690"),
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
                        // "--config-path",
                        // "src/config/risingwave.toml",
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
        tokio::time::sleep(Duration::from_secs(10)).await;

        // client
        let client = handle
            .create_node()
            .name("client")
            .ip([192, 168, 100, 1].into())
            .build();

        Ok(Self {
            meta,
            frontends,
            handle,
            client,
        })
    }

    pub async fn run(&mut self, sql: impl Into<String>) -> Result<()> {
        let frontend = self
            .frontends
            .choose(&mut thread_rng())
            .unwrap()
            .to_string();
        let sql: String = sql.into();

        self.client
            .spawn(async move {
                let mut session = Risingwave::connect(frontend, "dev".to_string()).await;
                let result = session.run(&sql).await?;
                println!("{result}");
                Ok::<_, anyhow::Error>(())
            })
            .await??;

        Ok(())
    }
}
