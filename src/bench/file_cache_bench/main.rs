// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::Parser;

#[cfg(target_os = "linux")]
mod analyze;
#[cfg(target_os = "linux")]
mod bench;
#[cfg(target_os = "linux")]
#[cfg(feature = "bpf")]
mod bpf;
#[cfg(target_os = "linux")]
mod rate;
#[cfg(target_os = "linux")]
mod utils;

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[clap(short, long)]
    path: String,
    /// (MiB)
    #[clap(long, default_value = "1024")]
    capacity: usize,
    /// (MiB)
    #[clap(long, default_value = "128")]
    total_buffer_capacity: usize,
    /// (MiB)
    #[clap(long, default_value = "512")]
    cache_file_fallocate_unit: usize,
    /// (MiB)
    #[clap(long, default_value = "16")]
    cache_meta_fallocate_unit: usize,
    /// (MiB)
    #[clap(long, default_value = "4")]
    cache_file_max_write_size: usize,

    /// (KiB)
    #[clap(long, default_value = "1024")]
    bs: usize,
    #[clap(long, default_value = "8")]
    concurrency: usize,
    /// (s)
    #[clap(long, default_value = "600")]
    time: u64,
    #[clap(long, default_value = "1")]
    read: usize,
    #[clap(long, default_value = "1")]
    write: usize,
    #[clap(long, default_value = "10000")]
    look_up_range: u32,
    /// (MiB/s), 0 for inf.
    #[clap(long, default_value = "0")]
    r_rate: f64,
    /// (MiB/s), 0 for inf.
    #[clap(long, default_value = "0")]
    w_rate: f64,
    /// (s)
    #[clap(long, default_value = "1")]
    report_interval: u64,
    /// Threshold to print linux I/O stack trace for slow reads iff feature `bpf` is enabled. (ms)
    #[clap(long, default_value = "100")]
    slow: u64,
    /// Endpoint for jaeger, only valid when feature `trace` is enabled.
    #[clap(long, default_value = "http://127.0.0.1:14268/api/traces")]
    jaeger_endpoint: String,
}

#[cfg(target_os = "linux")]
async fn main_okk() {
    use tokio::sync::oneshot;

    let args = Args::parse();

    #[cfg(feature = "trace")]
    {
        use isahc::config::Configurable;
        use tracing_subscriber::prelude::*;

        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        // Hint: Uncomment commented codes to debug tracing output.

        let filter = tracing_subscriber::filter::Targets::new()
            .with_target(
                "risingwave_storage::hummock::file_cache",
                tracing::Level::TRACE,
            )
            .with_target("file_cache_bench", tracing::Level::TRACE)
            .with_default(tracing::Level::WARN);
        // let fmt_layer = tracing_subscriber::fmt::layer()
        //     .with_ansi(true)
        //     .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        //     .with_writer(std::io::stdout)
        //     .with_filter(filter.clone());
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name("file-cache-bench")
            .with_collector_endpoint(&args.jaeger_endpoint)
            .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();
        let opentelemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter);
        tracing_subscriber::registry()
            // .with(fmt_layer)
            .with(opentelemetry_layer)
            .init();
    }

    let (bench_stop_tx, bench_stop_rx) = oneshot::channel();
    #[cfg(feature = "bpf")]
    let (bpf_stop_tx, bpf_stop_rx) = oneshot::channel();

    // Gracefully shutdown.
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        bench_stop_tx.send(()).unwrap();
        #[cfg(feature = "bpf")]
        bpf_stop_tx.send(()).unwrap();
    });

    #[cfg(feature = "bpf")]
    tokio::spawn(bpf::bpf(args.clone(), bpf_stop_rx));

    bench::run(args.clone(), bench_stop_rx).await;

    #[cfg(feature = "trace")]
    opentelemetry::global::shutdown_tracer_provider();
}

#[tokio::main]
async fn main() {
    if !cfg!(target_os = "linux") {
        panic!("only support linux")
    }

    #[cfg(target_os = "linux")]
    main_okk().await;
}
