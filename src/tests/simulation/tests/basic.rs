// #![cfg(madsim)]

use clap::Parser;

#[madsim::test]
async fn basic() {
    let handle = madsim::runtime::Handle::current();

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
    handle
        .create_node()
        .name("frontend")
        .ip("192.168.2.1".parse().unwrap())
        .init(|| async {
            let opts = risingwave_frontend::FrontendOpts::parse_from([
                "frontend-node",
                "--host",
                "0.0.0.0:4566",
                "--meta-addr",
                "192.168.1.1:5690",
            ]);
            risingwave_frontend::start(opts).await
        })
        .build();

    // compute node
    handle
        .create_node()
        .name("compute")
        .ip("192.168.3.1".parse().unwrap())
        .init(|| async {
            let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                "compute-node",
                "--host",
                "0.0.0.0:5688",
                "--meta-address",
                "192.168.1.1:5690",
            ]);
            risingwave_compute::start(opts).await
        })
        .build();

    tokio::signal::ctrl_c().await.unwrap();
}
