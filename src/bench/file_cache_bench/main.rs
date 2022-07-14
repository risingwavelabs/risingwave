#[cfg(target_os = "linux")]
mod bench;
#[cfg(target_os = "linux")]
mod utils;

#[tokio::main]
async fn main() {
    if !cfg!(target_os = "linux") {
        panic!("only support linux")
    }

    #[cfg(target_os = "linux")]
    bench::run().await;
}
