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

use std::borrow::Cow;
use std::env;
use std::path::PathBuf;

use either::Either;
use fastrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry::InstrumentationScope;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::TracerProviderBuilder;
use risingwave_common::util::deployment::Deployment;
use risingwave_common::util::env_var::env_var_is_true;
use risingwave_common::util::query_log::*;
use risingwave_common::util::tracing::layer::set_toggle_otel_layer_fn;
use risingwave_common::util::tracing::log_filter::{
    LogFilterControl, LogFilterError, set_log_filter_control,
};
use risingwave_pb::monitor_service::LogFilterState;
use thiserror_ext::AsReport;
use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, filter, reload};

/// Parse a comma-separated list of `key=value` pairs into `OpenTelemetry` `KeyValue` attributes.
///
/// - Splits by comma to get pairs.
/// - Each pair must contain exactly one `=`.
/// - Trims whitespace around key and value.
/// - Ignores invalid pairs but logs a warning.
/// - Returns an empty vec for empty or unset input.
fn parse_extra_tracing_attributes(input: &str) -> Vec<opentelemetry::KeyValue> {
    use opentelemetry::KeyValue;

    if input.trim().is_empty() {
        return vec![];
    }

    input
        .split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            if pair.is_empty() {
                return None;
            }
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() != 2 {
                eprintln!("warning: ignoring invalid tracing attribute pair (no '='): {pair:?}");
                return None;
            }
            let key = parts[0].trim();
            let value = parts[1].trim();
            if key.is_empty() {
                eprintln!("warning: ignoring tracing attribute with empty key: {pair:?}");
                return None;
            }
            Some(KeyValue::new(key.to_owned(), value.to_owned()))
        })
        .collect()
}

pub struct LoggerSettings {
    /// The name of the service. Used to identify the service in distributed tracing.
    name: String,
    /// Enable tokio console output.
    enable_tokio_console: bool,
    /// Enable colorful output in console.
    colorful: bool,
    /// Output to `stderr` instead of `stdout`.
    stderr: bool,
    /// Whether to include thread name in the log.
    with_thread_name: bool,
    /// Override target settings.
    targets: Vec<(String, tracing::metadata::LevelFilter)>,
    /// Override the default level.
    default_level: Option<tracing::metadata::LevelFilter>,
    /// The endpoint of the tracing collector in OTLP gRPC protocol.
    tracing_endpoint: Option<String>,
    /// Extra key/value attributes to attach to `OpenTelemetry` trace resources.
    extra_tracing_attributes: Vec<opentelemetry::KeyValue>,
}

impl Default for LoggerSettings {
    fn default() -> Self {
        Self::new("risingwave")
    }
}

impl LoggerSettings {
    /// Create a new logger settings from the given command-line options.
    ///
    /// If env var `RW_TRACING_ENDPOINT` is not set, the meta address will be used
    /// as the default tracing endpoint, which means that the embedded tracing
    /// collector will be used.
    pub fn from_opts<O: risingwave_common::opts::Opts>(opts: &O) -> Self {
        let mut settings = Self::new(O::name());
        if settings.tracing_endpoint.is_none() // no explicit endpoint
            && let Some(addr) = opts.meta_addr().exactly_one()
        // meta address is valid
        {
            // Use embedded collector in the meta service.
            // TODO: when there's multiple meta nodes for high availability, we may send
            // to a wrong node here.
            settings.tracing_endpoint = Some(addr.to_string());
        }
        settings
    }

    /// Create a new logger settings with the given service name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enable_tokio_console: false,
            colorful: console::colors_enabled_stderr() && console::colors_enabled(),
            stderr: false,
            with_thread_name: false,
            targets: vec![],
            default_level: None,
            tracing_endpoint: std::env::var("RW_TRACING_ENDPOINT").ok(),
            extra_tracing_attributes: std::env::var("RW_TRACING_EXTRA_ATTRIBUTES")
                .ok()
                .map(|v| parse_extra_tracing_attributes(&v))
                .unwrap_or_default(),
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

    /// Whether to include thread name in the log.
    pub fn with_thread_name(mut self, enabled: bool) -> Self {
        self.with_thread_name = enabled;
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

    /// Overrides the tracing endpoint.
    pub fn with_tracing_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.tracing_endpoint = Some(endpoint.into());
        self
    }
}

/// Create a filter that disables all events or spans.
fn disabled_filter() -> filter::Targets {
    filter::Targets::new()
}

fn apply_log_filter(mut defaults: Targets, overrides: Targets) -> Targets {
    // Targets carries its fallback level separately from its named target rules.
    // Iteration (and therefore with_targets) copies only the named rules.
    if let Some(level) = overrides.default_level() {
        defaults = defaults.with_default(level);
    }
    defaults.with_targets(overrides)
}

struct LogFilterController {
    // Built-in defaults plus LoggerSettings, excluding RUST_LOG.
    defaults: Targets,
    // Effective console filter at startup, restored by reset.
    startup: Targets,
    handle: reload::Handle<Targets, tracing_subscriber::Registry>,
    // Serialize updates, including interest-cache rebuilds, and state reads.
    state: parking_lot::Mutex<LogFilterState>,
}

impl LogFilterController {
    fn new(
        defaults: Targets,
        startup: Targets,
        handle: reload::Handle<Targets, tracing_subscriber::Registry>,
    ) -> Self {
        let startup_filter = startup.to_string();
        Self {
            defaults,
            startup,
            handle,
            state: parking_lot::Mutex::new(LogFilterState {
                startup_filter: startup_filter.clone(),
                current_filter: startup_filter,
            }),
        }
    }
}

impl LogFilterControl for LogFilterController {
    fn get(&self) -> LogFilterState {
        self.state.lock().clone()
    }

    fn set(&self, filter: Option<&str>) -> Result<LogFilterState, LogFilterError> {
        let next = match filter {
            Some(filter) => apply_log_filter(
                self.defaults.clone(),
                filter
                    .parse::<Targets>()
                    .map_err(|error| LogFilterError::InvalidFilter(error.to_string()))?,
            )
            // Keep tracing-only events out of stdout/stderr after applying overrides.
            .with_target("rw_tracing", Level::OFF),
            None => self.startup.clone(),
        };
        let mut state = self.state.lock();
        self.handle
            .reload(next.clone())
            .map_err(|error| LogFilterError::Reload(error.to_string()))?;
        state.current_filter = next.to_string();
        Ok(state.clone())
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
/// - Dump slow queries, i.e., tracing target [`PGWIRE_SLOW_QUERY_LOG`] to
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
///
/// ### `ENABLE_PRETTY_LOG`
///
/// If it is set to `true`, enable pretty log output, which contains line numbers and prints spans in multiple lines.
/// This can be helpful for development and debugging.
///
/// Hint: Also turn off other uninteresting logs to make the most of the pretty log.
/// e.g.,
/// ```bash
/// RUST_LOG="risingwave_storage::hummock::event_handler=off,batch_execute=off,risingwave_batch::task=off" ENABLE_PRETTY_LOG=true risedev d
/// ```
pub fn init_risingwave_logger(settings: LoggerSettings) {
    let deployment = Deployment::current();

    // Default timer for logging with local time offset.
    let default_timer = OffsetTime::local_rfc_3339().unwrap_or_else(|e| {
        println!(
            "failed to get local time offset, falling back to UTC: {}",
            e.as_report()
        );
        OffsetTime::new(
            time::UtcOffset::UTC,
            time::format_description::well_known::Rfc3339,
        )
    });

    // Default filter for logging to stdout and tracing.
    let default_filter = {
        let mut filter = filter::Targets::new();

        // Configure levels for some RisingWave crates. Can still be overridden by `RUST_LOG`.
        // Other RisingWave crates like `stream` and `storage` will follow the default level.
        filter = filter
            .with_target("auto_schema_change", Level::INFO)
            .with_target("risingwave_sqlparser", Level::INFO)
            .with_target("risingwave_connector_node", Level::INFO)
            .with_target("pgwire", Level::INFO)
            .with_target(PGWIRE_QUERY_LOG, Level::OFF)
            // debug-purposed events are disabled unless `RUST_LOG` overrides
            .with_target("events", Level::OFF);

        // Configure levels for external crates. Can still be overridden by `RUST_LOG`.
        // Other external crates will follow the default level.
        filter = filter
            .with_target("foyer", Level::INFO)
            .with_target("aws", Level::INFO)
            .with_target("aws_config", Level::WARN)
            .with_target("aws_endpoint", Level::WARN)
            .with_target("aws_credential_types::cache::lazy_caching", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("tonic", Level::WARN)
            .with_target("isahc", Level::WARN)
            .with_target("console_subscriber", Level::WARN)
            .with_target("reqwest", Level::WARN)
            .with_target("sled", Level::INFO)
            .with_target("cranelift", Level::INFO)
            .with_target("wasmtime", Level::INFO)
            .with_target("sqlx", Level::WARN)
            .with_target("opendal", Level::INFO)
            .with_target("reqsign", Level::INFO)
            .with_target("jni", Level::INFO)
            .with_target("async_nats", Level::WARN);

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
        filter.with_default(default_level)
    };

    let mut settings_filter = Targets::new().with_targets(settings.targets);
    if let Some(default_level) = settings.default_level {
        settings_filter = settings_filter.with_default(default_level);
    }

    let fmt_defaults = apply_log_filter(default_filter.clone(), settings_filter.clone());

    // Preserve defaults separately so each runtime set replaces RUST_LOG, while
    // reset restores the exact startup filter. Keep the exporter filter independent.
    let rust_log_filter = match std::env::var(EnvFilter::DEFAULT_ENV) {
        Ok(rust_log) if !rust_log.is_empty() => {
            rust_log.parse().expect("failed to parse `RUST_LOG`")
        }
        _ => Targets::new(),
    };
    // Exclude tracing-only instrumentation after all console overrides. The
    // exporter keeps its own filter without this exclusion.
    let fmt_startup = apply_log_filter(fmt_defaults.clone(), rust_log_filter.clone())
        .with_target("rw_tracing", Level::OFF);
    #[cfg(not(madsim))]
    let otel_startup_filter = apply_log_filter(
        apply_log_filter(default_filter, settings_filter),
        rust_log_filter,
    );

    let mut layers = vec![];

    // fmt layer (formatting and logging to `stdout` or `stderr`)
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_names(settings.with_thread_name)
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
            Deployment::Ci => fmt_layer.compact().boxed(),
            Deployment::Cloud => fmt_layer
                .json()
                .map_event_format(|e| e.with_current_span(false)) // avoid duplication as there's a span list field
                .boxed(),
            Deployment::Other => {
                if env_var_is_true("ENABLE_PRETTY_LOG") {
                    fmt_layer.pretty().boxed()
                } else {
                    fmt_layer.boxed()
                }
            }
        };

        let (filter, handle) = reload::Layer::new(fmt_startup.clone());
        set_log_filter_control(LogFilterController::new(fmt_defaults, fmt_startup, handle));
        layers.push(fmt_layer.with_filter(filter).boxed());
    };

    // If `RW_QUERY_LOG_PATH` env var is set to a directory, turn on query log files.
    let query_log_path = std::env::var("RW_QUERY_LOG_PATH");
    if let Ok(query_log_path) = query_log_path {
        let query_log_path = PathBuf::from(query_log_path);
        std::fs::create_dir_all(query_log_path.clone()).unwrap_or_else(|e| {
            panic!(
                "failed to create directory '{}' for query log: {}",
                query_log_path.display(),
                e.as_report(),
            )
        });

        /// Newtype wrapper for `DefaultFields`.
        ///
        /// `fmt::Layer` will share the same `FormattedFields` extension for spans across
        /// different layers, as long as the type of `N: FormatFields` is the same. This
        /// will cause several problems:
        ///
        /// - `with_ansi(false)` does not take effect and it will follow the settings of
        ///   the primary fmt layer installed above.
        /// - `Span::record` will update the same `FormattedFields` multiple times,
        ///   leading to duplicated fields.
        ///
        /// As a workaround, we use a newtype wrapper here to get a different type id.
        /// The const generic parameter `SLOW` is further used to distinguish between the
        /// query log and the slow query log.
        #[derive(Default)]
        struct FmtFields<const SLOW: bool>(DefaultFields);

        impl<'writer, const SLOW: bool> FormatFields<'writer> for FmtFields<SLOW> {
            fn format_fields<R: tracing_subscriber::field::RecordFields>(
                &self,
                writer: tracing_subscriber::fmt::format::Writer<'writer>,
                fields: R,
            ) -> std::fmt::Result {
                self.0.format_fields(writer, fields)
            }
        }

        for (file_name, target, is_slow) in [
            ("query.log", PGWIRE_QUERY_LOG, false),
            ("slow_query.log", PGWIRE_SLOW_QUERY_LOG, true),
        ] {
            let path = query_log_path.join(file_name);

            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap_or_else(|e| {
                    panic!("failed to create `{}`: {}", path.display(), e.as_report(),)
                });

            let layer = tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_level(false)
                .with_file(false)
                .with_target(false)
                .with_timer(default_timer.clone())
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_writer(file);

            let layer = match is_slow {
                true => layer.fmt_fields(FmtFields::<true>::default()).boxed(),
                false => layer.fmt_fields(FmtFields::<false>::default()).boxed(),
            };

            let layer = layer.with_filter(
                filter::Targets::new()
                    // Root span must be enabled to provide common info like the SQL query.
                    .with_target(PGWIRE_ROOT_SPAN_TARGET, Level::INFO)
                    .with_target(target, Level::INFO),
            );

            layers.push(layer.boxed());
        }
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
    if let Some(endpoint) = settings.tracing_endpoint {
        println!("opentelemetry tracing will be exported to `{endpoint}` if enabled");

        use opentelemetry::KeyValue;
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

        let extra_attributes = &settings.extra_tracing_attributes;
        if !extra_attributes.is_empty() {
            println!(
                "extra tracing resource attributes: {:?}",
                extra_attributes
                    .iter()
                    .map(|kv| format!("{}={}", kv.key, kv.value))
                    .collect::<Vec<_>>()
            );
        }

        let (otel_tracer, exporter) = {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("rw-otel")
                .worker_threads(2)
                .build()
                .unwrap();
            let runtime = Box::leak(Box::new(runtime));

            // Installing the exporter requires a tokio runtime.
            let _entered = runtime.enter();

            // TODO(bugen): better service name
            // https://github.com/jaegertracing/jaeger-ui/issues/336
            let service_name = format!("{}-{}", settings.name, id);

            let mut resource_attrs = vec![
                KeyValue::new(resource::SERVICE_NAME, service_name.clone()),
                KeyValue::new(resource::SERVICE_INSTANCE_ID, id.clone()),
                KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
            ];
            resource_attrs.extend(extra_attributes.iter().cloned());

            let otel_tracer = TracerProviderBuilder::default()
                .with_batch_exporter(
                    SpanExporter::builder()
                        .with_tonic()
                        .with_endpoint(&endpoint)
                        .build()
                        .unwrap(),
                )
                .with_resource(Resource::builder().with_attributes(resource_attrs).build())
                .build()
                .tracer(service_name);

            let exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                .with_timeout(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT)
                .build()
                .unwrap();

            (otel_tracer, exporter)
        };

        // Disable by filtering out all events or spans by default.
        //
        // It'll be enabled with `toggle_otel_layer` based on the system parameter `enable_tracing` later.
        let (reload_filter, reload_handle) = reload::Layer::new(disabled_filter());

        set_toggle_otel_layer_fn(move |enabled: bool| {
            let result = reload_handle.modify(|f| {
                *f = if enabled {
                    otel_startup_filter.clone()
                } else {
                    disabled_filter()
                }
            });

            match result {
                Ok(_) => tracing::info!(
                    "opentelemetry tracing {}",
                    if enabled { "enabled" } else { "disabled" },
                ),

                Err(error) => tracing::error!(
                    error = %error.as_report(),
                    "failed to {} opentelemetry tracing",
                    if enabled { "enable" } else { "disable" },
                ),
            }
        });

        let layer = tracing_opentelemetry::layer()
            .with_tracer(otel_tracer)
            .with_filter(reload_filter);

        layers.push(layer.boxed());

        // The reporter is used by fastrace in foyer for dynamically tail-based tracing.
        //
        // Code here only setup the OpenTelemetry reporter. To enable/disable the function, please use risectl.
        //
        // e.g.
        //
        // ```bash
        // risectl hummock tiered-cache-tracing -h
        // ```
        let mut fastrace_resource_attrs: Vec<opentelemetry::KeyValue> =
            vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                format!("fastrace-{id}"),
            )];
        fastrace_resource_attrs.extend(extra_attributes.iter().cloned());

        let reporter = OpenTelemetryReporter::new(
            exporter,
            Cow::Owned(
                Resource::builder()
                    .with_attributes(fastrace_resource_attrs)
                    .build(),
            ),
            InstrumentationScope::builder("opentelemetry-instrumentation-foyer").build(),
        );
        fastrace::set_reporter(reporter, fastrace::collector::Config::default());
        tracing::info!("opentelemetry exporter for fastrace is set at {endpoint}");
    }

    tracing_subscriber::registry().with(layers).init();
    // TODO: add file-appender tracing subscriber in the future
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_log_filter() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        fn emit(evaluations: &AtomicUsize) {
            tracing::debug!(target: "dynamic_log_test", value = {
                evaluations.fetch_add(1, Ordering::Relaxed);
                "expensive value"
            });
        }

        fn emit_tracing(evaluations: &AtomicUsize) {
            tracing::info!(target: "rw_tracing", value = {
                evaluations.fetch_add(1, Ordering::Relaxed);
                "tracing-only event"
            });
        }

        // Explicit LoggerSettings and RUST_LOG directives cannot enable the
        // tracing-only target on the console.
        let defaults = Targets::new()
            .with_default(Level::INFO)
            .with_target("quiet_dependency", Level::WARN)
            .with_target("rw_tracing", Level::DEBUG);
        let startup = apply_log_filter(
            defaults.clone(),
            "warn,startup_only=debug,rw_tracing=info".parse().unwrap(),
        );
        // The shared merge still allows this target for OpenTelemetry.
        assert!(startup.would_enable("rw_tracing", &tracing::Level::INFO));
        let startup = startup.with_target("rw_tracing", Level::OFF);
        assert!(!startup.would_enable("rw_tracing", &tracing::Level::ERROR));
        let (filter, handle) = reload::Layer::new(startup.clone());
        let controller = LogFilterController::new(defaults, startup, handle);
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::sink)
                .with_filter(filter),
        );
        let _guard = tracing::subscriber::set_default(subscriber);
        let evaluations = AtomicUsize::new(0);
        let tracing_evaluations = AtomicUsize::new(0);

        emit(&evaluations);
        assert_eq!(evaluations.load(Ordering::Relaxed), 0);
        emit_tracing(&tracing_evaluations);
        assert_eq!(tracing_evaluations.load(Ordering::Relaxed), 0);

        // Runtime directives replace startup RUST_LOG, retaining only the defaults.
        let state = controller.set(Some("info")).unwrap();
        let current: Targets = state.current_filter.parse().unwrap();
        assert!(!current.would_enable("startup_only", &tracing::Level::DEBUG));
        emit_tracing(&tracing_evaluations);
        assert_eq!(tracing_evaluations.load(Ordering::Relaxed), 0);

        // A global level change alone does not enable tracing-only events.
        controller.set(Some("debug")).unwrap();
        emit_tracing(&tracing_evaluations);
        assert_eq!(tracing_evaluations.load(Ordering::Relaxed), 0);

        let state = controller.set(Some("dynamic_log_test=debug")).unwrap();
        let current: Targets = state.current_filter.parse().unwrap();
        assert_eq!(current.default_level(), Some(Level::INFO));
        emit(&evaluations);
        assert_eq!(evaluations.load(Ordering::Relaxed), 1);

        // Invalid directives must leave both the active filter and reported state intact.
        assert!(matches!(
            controller.set(Some("dynamic_log_test=invalid")),
            Err(LogFilterError::InvalidFilter(_))
        ));
        assert_eq!(controller.get(), state);
        emit(&evaluations);
        assert_eq!(evaluations.load(Ordering::Relaxed), 2);

        // A new update replaces previous directives but cannot enable rw_tracing.
        let state = controller
            .set(Some("warn,other_target=debug,rw_tracing=debug"))
            .unwrap();
        emit(&evaluations);
        assert_eq!(evaluations.load(Ordering::Relaxed), 2);
        let current: Targets = state.current_filter.parse().unwrap();
        assert_eq!(current.default_level(), Some(Level::WARN));
        assert!(current.would_enable("other_target", &tracing::Level::DEBUG));
        assert!(!current.would_enable("rw_tracing", &tracing::Level::ERROR));
        assert!(!current.would_enable("quiet_dependency", &tracing::Level::INFO));
        emit_tracing(&tracing_evaluations);
        assert_eq!(tracing_evaluations.load(Ordering::Relaxed), 0);

        controller.set(Some("dynamic_log_test=debug")).unwrap();
        emit(&evaluations);
        assert_eq!(evaluations.load(Ordering::Relaxed), 3);
        emit_tracing(&tracing_evaluations);
        assert_eq!(tracing_evaluations.load(Ordering::Relaxed), 0);
        let reset = controller.set(None).unwrap();
        assert_eq!(reset.current_filter, reset.startup_filter);
        emit(&evaluations);
        assert_eq!(evaluations.load(Ordering::Relaxed), 3);
        let current: Targets = reset.current_filter.parse().unwrap();
        assert_eq!(current.default_level(), Some(Level::WARN));
        assert!(current.would_enable("startup_only", &tracing::Level::DEBUG));
        emit_tracing(&tracing_evaluations);
        assert_eq!(tracing_evaluations.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_parse_extra_tracing_attributes_normal() {
        let attrs =
            parse_extra_tracing_attributes("cluster=prod,region=us-east-1,namespace=terry-dev");
        assert_eq!(attrs.len(), 3);
        assert_eq!(attrs[0].key.as_str(), "cluster");
        assert_eq!(attrs[0].value.as_str(), "prod");
        assert_eq!(attrs[1].key.as_str(), "region");
        assert_eq!(attrs[1].value.as_str(), "us-east-1");
        assert_eq!(attrs[2].key.as_str(), "namespace");
        assert_eq!(attrs[2].value.as_str(), "terry-dev");
    }

    #[test]
    fn test_parse_extra_tracing_attributes_whitespace() {
        let attrs = parse_extra_tracing_attributes("  key1 = val1 , key2=val2  ");
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].key.as_str(), "key1");
        assert_eq!(attrs[0].value.as_str(), "val1");
        assert_eq!(attrs[1].key.as_str(), "key2");
        assert_eq!(attrs[1].value.as_str(), "val2");
    }

    #[test]
    fn test_parse_extra_tracing_attributes_empty() {
        assert!(parse_extra_tracing_attributes("").is_empty());
        assert!(parse_extra_tracing_attributes("  ").is_empty());
    }

    #[test]
    fn test_parse_extra_tracing_attributes_invalid_pairs() {
        // No '=' sign — should be skipped
        let attrs = parse_extra_tracing_attributes("good=value,badpair,also_good=123");
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].key.as_str(), "good");
        assert_eq!(attrs[1].key.as_str(), "also_good");
    }

    #[test]
    fn test_parse_extra_tracing_attributes_empty_key() {
        // "=value" has empty key — should be skipped
        let attrs = parse_extra_tracing_attributes("=value,key=val");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key.as_str(), "key");
    }

    #[test]
    fn test_parse_extra_tracing_attributes_value_with_equals() {
        // value itself contains '=' — only first '=' is the delimiter
        let attrs = parse_extra_tracing_attributes("expr=a=b");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key.as_str(), "expr");
        assert_eq!(attrs[0].value.as_str(), "a=b");
    }

    #[test]
    fn test_parse_extra_tracing_attributes_trailing_comma() {
        let attrs = parse_extra_tracing_attributes("k1=v1,k2=v2,");
        assert_eq!(attrs.len(), 2);
    }

    #[test]
    fn test_parse_extra_tracing_attributes_empty_value() {
        let attrs = parse_extra_tracing_attributes("key=");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key.as_str(), "key");
        assert_eq!(attrs[0].value.as_str(), "");
    }
}
