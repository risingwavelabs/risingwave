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

use std::env;
use std::path::PathBuf;

use either::Either;
use risingwave_common::metrics::MetricsLayer;
use risingwave_common::util::deployment::Deployment;
use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, EnvFilter};

const PGWIRE_QUERY_LOG: &str = "pgwire_query_log";
const SLOW_QUERY_LOG: &str = "risingwave_frontend_slow_query_log";

pub struct LoggerSettings {
    /// The name of the service.
    name: String,
    /// Enable tokio console output.
    enable_tokio_console: bool,
    /// Enable colorful output in console.
    colorful: bool,
    /// Output to `stderr` instead of `stdout`.
    stderr: bool,
    /// Override target settings.
    targets: Vec<(String, tracing::metadata::LevelFilter)>,
    /// Override the default level.
    default_level: Option<tracing::metadata::LevelFilter>,
}

impl Default for LoggerSettings {
    fn default() -> Self {
        Self::new("risingwave")
    }
}

impl LoggerSettings {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enable_tokio_console: false,
            colorful: console::colors_enabled_stderr() && console::colors_enabled(),
            stderr: false,
            targets: vec![],
            default_level: None,
        }
    }

    /// Enable tokio console output.
    pub fn tokio_console(mut self, enabled: bool) -> Self {
        self.enable_tokio_console = enabled;
        self
    }

    /// Output to `stderr` instead of `stdout`.
    pub fn stderr(mut self, enabled: bool) -> Self {
        self.stderr = enabled;
        self
    }

    /// Overrides the default target settings.
    pub fn with_target(
        mut self,
        target: impl Into<String>,
        level: impl Into<tracing::metadata::LevelFilter>,
    ) -> Self {
        self.targets.push((target.into(), level.into()));
        self
    }

    /// Overrides the default level.
    pub fn with_default(mut self, level: impl Into<tracing::metadata::LevelFilter>) -> Self {
        self.default_level = Some(level.into());
        self
    }
}

/// Init logger for RisingWave binaries.
///
/// ## Environment variables to configure logger dynamically
///
/// ### `RUST_LOG`
///
/// Overrides default level and tracing targets of the fmt layer (formatting and
/// logging to `stdout` or `stderr`).
///
/// Note that only verbosity levels below or equal to `DEBUG` are effective in
/// release builds.
///
/// e.g.,
/// ```bash
/// RUST_LOG="info,risingwave_stream=debug,events=debug"
/// ```
///
/// ### `RW_QUERY_LOG_PATH`
///
/// Configures the path to generate query log.
///
/// If it is set,
/// - Dump logs of all SQLs, i.e., tracing target [`PGWIRE_QUERY_LOG`] to
///   `RW_QUERY_LOG_PATH/query.log`.
/// - Dump slow queries, i.e., tracing target [`SLOW_QUERY_LOG`] to
///   `RW_QUERY_LOG_PATH/slow_query.log`.
///
/// Note:
/// To enable query log in the fmt layer (slow query is included by default), set
/// ```bash
/// RUST_LOG="pgwire_query_log=info"
/// ```
///
/// `RW_QUERY_LOG_TRUNCATE_LEN` configures the max length of the SQLs logged in the query log,
/// to avoid the log file growing too large. The default value is 1024 in production.
pub fn init_risingwave_logger(settings: LoggerSettings) {
    let deployment = Deployment::current();

    // Default timer for logging with local time offset.
    let default_timer = OffsetTime::local_rfc_3339().unwrap_or_else(|e| {
        println!("failed to get local time offset: {e}, falling back to UTC");
        OffsetTime::new(
            time::UtcOffset::UTC,
            time::format_description::well_known::Rfc3339,
        )
    });

    // Default filter for logging to stdout and tracing.
    let default_filter = {
        let mut filter = filter::Targets::new();

        // Configure levels for some RisingWave crates.
        // Other RisingWave crates like `stream` and `storage` will follow the default level.
        filter = filter
            .with_target("risingwave_sqlparser", Level::INFO)
            .with_target("pgwire", Level::INFO)
            .with_target(PGWIRE_QUERY_LOG, Level::OFF)
            // debug-purposed events are disabled unless `RUST_LOG` overrides
            .with_target("events", Level::OFF);

        // Configure levels for external crates.
        filter = filter
            .with_target("foyer", Level::WARN)
            .with_target("aws_sdk_ec2", Level::INFO)
            .with_target("aws_sdk_s3", Level::INFO)
            .with_target("aws_config", Level::WARN)
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("aws_endpoint", Level::WARN)
            .with_target("aws_credential_types::cache::lazy_caching", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("tonic", Level::WARN)
            .with_target("isahc", Level::WARN)
            .with_target("console_subscriber", Level::WARN)
            .with_target("reqwest", Level::WARN)
            .with_target("sled", Level::INFO);

        // For all other crates, apply default level depending on the deployment and `debug_assertions` flag.
        let default_level = match deployment {
            Deployment::Ci => Level::INFO,
            _ => {
                if cfg!(debug_assertions) {
                    Level::DEBUG
                } else {
                    Level::INFO
                }
            }
        };
        filter = filter.with_default(default_level);

        // Overrides from settings.
        filter = filter.with_targets(settings.targets);
        if let Some(default_level) = settings.default_level {
            filter = filter.with_default(default_level);
        }

        // Overrides from env var.
        if let Ok(rust_log) = std::env::var(EnvFilter::DEFAULT_ENV)
            && !rust_log.is_empty()
        {
            let rust_log_targets: Targets = rust_log.parse().expect("failed to parse `RUST_LOG`");
            if let Some(default_level) = rust_log_targets.default_level() {
                filter = filter.with_default(default_level);
            }
            filter = filter.with_targets(rust_log_targets)
        };

        filter
    };

    let mut layers = vec![];

    // fmt layer (formatting and logging to `stdout` or `stderr`)
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_timer(default_timer.clone())
            .with_ansi(settings.colorful)
            .with_writer(move || {
                if settings.stderr {
                    Either::Left(std::io::stderr())
                } else {
                    Either::Right(std::io::stdout())
                }
            });

        let fmt_layer = match deployment {
            Deployment::Ci => fmt_layer
                .compact()
                .with_filter(FilterFn::new(|metadata| metadata.is_event())) // filter-out all span-related info
                .boxed(),
            Deployment::Cloud => fmt_layer.json().boxed(),
            Deployment::Other => fmt_layer.boxed(),
        };

        layers.push(
            fmt_layer
                .with_filter(default_filter.clone().with_target("rw_tracing", Level::OFF)) // filter-out tracing-only events
                .boxed(),
        );
    };

    // If `RW_QUERY_LOG_PATH` env var is set to a directory, turn on query log files.
    let query_log_path = std::env::var("RW_QUERY_LOG_PATH");
    if let Ok(query_log_path) = query_log_path {
        let query_log_path = PathBuf::from(query_log_path);
        std::fs::create_dir_all(query_log_path.clone()).unwrap_or_else(|e| {
            panic!(
                "failed to create directory '{}' for query log: {e}",
                query_log_path.display()
            )
        });
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(query_log_path.join("query.log"))
            .unwrap_or_else(|e| {
                panic!(
                    "failed to create '{}/query.log': {e}",
                    query_log_path.display()
                )
            });
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_level(false)
            .with_file(false)
            .with_target(false)
            .with_timer(default_timer.clone())
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_writer(std::sync::Mutex::new(file))
            .with_filter(filter::Targets::new().with_target(PGWIRE_QUERY_LOG, Level::TRACE));
        layers.push(layer.boxed());

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(query_log_path.join("slow_query.log"))
            .unwrap_or_else(|e| {
                panic!(
                    "failed to create '{}/slow_query.log': {e}",
                    query_log_path.display()
                )
            });
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_level(false)
            .with_file(false)
            .with_target(false)
            .with_timer(default_timer)
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_writer(std::sync::Mutex::new(file))
            .with_filter(filter::Targets::new().with_target(SLOW_QUERY_LOG, Level::TRACE));
        layers.push(layer.boxed());
    }

    if settings.enable_tokio_console {
        let (console_layer, server) = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .build();
        let console_layer = console_layer.with_filter(
            filter::Targets::new()
                .with_target("tokio", Level::TRACE)
                .with_target("runtime", Level::TRACE),
        );
        layers.push(console_layer.boxed());
        std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    println!("serving console subscriber");
                    server.serve().await.unwrap();
                });
        });
    };

    // Tracing layer
    #[cfg(not(madsim))]
    if let Ok(endpoint) = std::env::var("RW_TRACING_ENDPOINT") {
        println!("tracing enabled, exported to `{endpoint}`");

        use opentelemetry::{sdk, KeyValue};
        use opentelemetry_otlp::WithExportConfig;
        use opentelemetry_semantic_conventions::resource;

        let id = format!(
            "{}-{}",
            hostname::get()
                .ok()
                .and_then(|o| o.into_string().ok())
                .unwrap_or_default(),
            std::process::id()
        );

        let otel_tracer = {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("risingwave-otel")
                .worker_threads(2)
                .build()
                .unwrap();
            let runtime = Box::leak(Box::new(runtime));

            // Installing the exporter requires a tokio runtime.
            let _entered = runtime.enter();

            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(endpoint),
                )
                .with_trace_config(sdk::trace::config().with_resource(sdk::Resource::new([
                    KeyValue::new(
                        resource::SERVICE_NAME,
                        // TODO(bugen): better service name
                        // https://github.com/jaegertracing/jaeger-ui/issues/336
                        format!("{}-{}", settings.name, id),
                    ),
                    KeyValue::new(resource::SERVICE_INSTANCE_ID, id),
                    KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                    KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
                ])))
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap()
        };

        let layer = tracing_opentelemetry::layer()
            .with_tracer(otel_tracer)
            .with_filter(default_filter);

        layers.push(layer.boxed());
    }

    // Metrics layer
    {
        let filter = filter::Targets::new().with_target("aws_smithy_client::retry", Level::DEBUG);

        layers.push(Box::new(MetricsLayer::new().with_filter(filter)));
    }
    tracing_subscriber::registry().with(layers).init();
    // TODO: add file-appender tracing subscriber in the future
}
