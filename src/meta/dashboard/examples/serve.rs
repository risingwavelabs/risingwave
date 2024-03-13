use risingwave_meta_dashboard::router;

#[tokio::main]
async fn main() {
    axum::Server::bind(&"0.0.0.0:10188".parse().unwrap())
        .serve(router().into_make_service())
        .await
        .unwrap();
}
