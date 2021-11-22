use risingwave_regress_test::regress_main;

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    regress_main().await
}
