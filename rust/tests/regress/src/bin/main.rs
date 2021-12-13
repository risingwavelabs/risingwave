use std::process::exit;

use risingwave_regress_test::regress_main;

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    exit(regress_main().await)
}
