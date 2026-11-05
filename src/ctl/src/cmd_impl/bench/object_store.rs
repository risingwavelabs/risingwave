// Copyright 2026 RisingWave Labs
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

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use risingwave_common::config::{ObjectStoreConfig, RwConfig};
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_object_store::benchmark::{self, BenchmarkConfig, Profile};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use serde_json::json;

#[derive(Args, Debug)]
pub struct ObjectStoreArgs {
    /// Store URL (`s3://bucket`); otherwise use `RW_HUMMOCK_URL`, stripping `hummock+`.
    #[arg(long)]
    pub store: Option<String>,
    /// Existing risingwave.toml. Read settings from `storage.object_store`.
    #[arg(long)]
    pub config_path: Option<PathBuf>,
    /// JSON `BenchmarkConfig`. CLI tuning options override this file.
    #[arg(long)]
    pub parameters: Option<PathBuf>,
    /// Workload profile (default: all).
    #[arg(long, value_parser = ["basic", "compactor", "all"])]
    pub profile: Option<String>,
    /// Large-object count (default: 16).
    #[arg(long)]
    pub objects: Option<usize>,
    /// Size of each large object, in bytes or KiB/MiB/GiB (default: 64MiB).
    #[arg(long, value_parser = parse_size)]
    pub object_size: Option<usize>,
    /// Concurrent workers; also the competitor count in mixed experiments (default: 8).
    #[arg(long)]
    pub concurrency: Option<usize>,
    /// Basic ranged GET and fast-reader size (default: 1MiB).
    #[arg(long, value_parser = parse_size)]
    pub range_size: Option<usize>,
    /// Range/block operations per object in fixed-work basic mode (default: 16).
    #[arg(long)]
    pub read_iterations: Option<usize>,
    /// Wall-time budget per basic phase; 0 uses fixed counts (default: 30000 ms).
    #[arg(long)]
    pub basic_duration_ms: Option<u64>,
    /// Maximum logical attempts per basic phase, including failures (default: 10000).
    #[arg(long)]
    pub basic_max_operations: Option<usize>,
    /// Basic random block GET size (default: 64KiB).
    #[arg(long, value_parser = parse_size)]
    pub block_size: Option<usize>,
    /// Application consumption unit, independent of GET size (default: 256KiB).
    #[arg(long, value_parser = parse_size)]
    pub consume_size: Option<usize>,
    /// Pause after each nonfinal consumption unit (default: 10 ms).
    #[arg(long)]
    pub pause_ms: Option<u64>,
    /// Add deterministic jitter in 0..=N milliseconds (default: 0).
    #[arg(long)]
    pub jitter_ms: Option<u64>,
    /// Add a long pause every N consumption units; 0 disables it (default: 0).
    #[arg(long)]
    pub long_pause_every: Option<usize>,
    /// Additional long-pause duration (default: 1000 ms).
    #[arg(long)]
    pub long_pause_ms: Option<u64>,
    /// GET size for the chunked comparison (default: 4MiB).
    #[arg(long, value_parser = parse_size)]
    pub short_get_size: Option<usize>,
    /// 4KiB objects for LIST/DELETE (default: 1000).
    #[arg(long)]
    pub metadata_objects: Option<usize>,
    /// Existing objects per measured delete batch, at most 1000 (default: 100).
    #[arg(long)]
    pub delete_batch_size: Option<usize>,
    /// Fixed fast cohort size in interference experiments (default: 2).
    #[arg(long)]
    pub fast_readers: Option<usize>,
    /// Common observation window for each interference experiment (default: 10000 ms).
    #[arg(long)]
    pub interference_ms: Option<u64>,
    /// Reproducible payload, offset, and jitter seed (default: 42).
    #[arg(long)]
    pub seed: Option<u64>,
    /// Retain test data for inspection. Batch DELETE still deletes its own dataset.
    #[arg(long)]
    pub keep_data: bool,
    /// Reuse the `data/<index>` objects from a retained prefix (compactor profile only).
    #[arg(long)]
    pub reuse_prefix: Option<String>,
    /// Override the configured S3 backend, recording the effective value in the report.
    #[arg(long, value_parser = ["opendal", "aws"])]
    pub backend: Option<String>,
    /// Cooperative deadline; an in-flight write is allowed to finish or hit SDK timeout.
    #[arg(long, default_value_t = 1800)]
    pub max_runtime_secs: u64,
    /// Also save JSON atomically here; the parent directory must exist.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

fn parse_size(input: &str) -> Result<usize, String> {
    let (number, multiplier) = [
        ("GiB", 1usize << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ]
    .into_iter()
    .find_map(|(suffix, size)| input.strip_suffix(suffix).map(|n| (n, size)))
    .unwrap_or((input, 1));
    number
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .ok_or_else(|| "expected bytes or an integer followed by KiB, MiB, or GiB".into())
}

fn redact_url(url: &str) -> String {
    match url.rsplit_once('@') {
        Some((before, after)) => format!(
            "{}[redacted]@{after}",
            before
                .split_once("://")
                .map_or("", |(scheme, _)| if scheme == "minio" {
                    "minio://"
                } else {
                    "[url]://"
                })
        ),
        None => url.split('?').next().unwrap_or(url).to_owned(),
    }
}

fn redact_strings(value: &mut serde_json::Value, secrets: &[String]) {
    match value {
        serde_json::Value::String(text) => {
            for secret in secrets {
                if !secret.is_empty() {
                    *text = text.replace(secret, "[redacted]");
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_strings(item, secrets);
            }
        }
        serde_json::Value::Object(items) => {
            for item in items.values_mut() {
                redact_strings(item, secrets);
            }
        }
        _ => {}
    }
}

fn redact_errors(value: &mut serde_json::Value, secrets: &[String]) {
    match value {
        serde_json::Value::Object(items) => {
            for (key, item) in items {
                if key == "errors" || key == "cleanup_errors" {
                    redact_strings(item, secrets);
                } else {
                    redact_errors(item, secrets);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_errors(item, secrets);
            }
        }
        _ => {}
    }
}

pub async fn run(args: ObjectStoreArgs, shutdown: CancellationToken) -> Result<()> {
    let mut parameters: BenchmarkConfig = if let Some(path) = &args.parameters {
        serde_json::from_slice(
            &tokio::fs::read(path)
                .await
                .context("read benchmark parameters")?,
        )
        .context("parse benchmark parameters")?
    } else {
        BenchmarkConfig::default()
    };
    if let Some(profile) = args.profile {
        parameters.profile = match profile.as_str() {
            "basic" => Profile::Basic,
            "compactor" => Profile::Compactor,
            _ => Profile::All,
        };
    }
    macro_rules! override_parameters {
        ($($field:ident),* $(,)?) => { $(if let Some(value) = args.$field { parameters.$field = value; })* };
    }
    override_parameters!(
        objects,
        object_size,
        concurrency,
        range_size,
        read_iterations,
        basic_duration_ms,
        basic_max_operations,
        block_size,
        consume_size,
        pause_ms,
        jitter_ms,
        long_pause_every,
        long_pause_ms,
        short_get_size,
        metadata_objects,
        delete_batch_size,
        fast_readers,
        interference_ms,
        seed
    );
    parameters.keep_data |= args.keep_data;
    if args.reuse_prefix.is_some() {
        parameters.reuse_data = true;
        parameters.keep_data = true;
    }
    ensure!(
        !parameters.reuse_data || args.reuse_prefix.is_some(),
        "reuse_data requires --reuse-prefix"
    );
    parameters.validate()?;
    ensure!(
        args.max_runtime_secs > 0,
        "max-runtime-secs must be positive"
    );
    ensure!(
        tokio::time::Instant::now()
            .checked_add(Duration::from_secs(args.max_runtime_secs))
            .is_some(),
        "max-runtime-secs exceeds the platform clock range"
    );
    // Probe the destination before S3 I/O, without truncating an existing report.
    // A sibling temporary file also makes the final replacement atomic.
    let output = args
        .output
        .as_ref()
        .map(|path| {
            ensure!(
                !path.is_dir(),
                "report output is a directory: {}",
                path.display()
            );
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let file = tempfile::Builder::new()
                .prefix(".object-store-report-")
                .tempfile_in(parent)
                .context("prepare benchmark report output")?;
            Ok::<_, anyhow::Error>((file, path))
        })
        .transpose()?;

    let mut object_config: ObjectStoreConfig = if let Some(path) = &args.config_path {
        let text = tokio::fs::read_to_string(path)
            .await
            .context("read RW config")?;
        let config: RwConfig = toml::from_str(&text).context("parse RW config")?;
        config.storage.object_store
    } else {
        ObjectStoreConfig::default()
    };
    if let Some(backend) = &args.backend {
        object_config.s3.developer.use_opendal = backend == "opendal";
    }
    // Diagnostics should abort failed OpenDAL multipart uploads when supported.
    object_config.opendal_writer_abort_on_err = true;
    if parameters.profile != Profile::Basic && object_config.s3.developer.use_opendal {
        let requested = parameters.fast_readers + parameters.concurrency;
        for (name, limit) in [
            ("req_concurrency_limit", object_config.req_concurrency_limit),
            ("http_concurrent_limit", object_config.http_concurrent_limit),
        ] {
            ensure!(
                limit == 0 || limit >= requested,
                "{name}={limit} cannot accommodate {requested} readers during readiness; lower benchmark concurrency or explicitly change the object-store limit"
            );
        }
    }
    if parameters.profile != Profile::Compactor {
        ensure!(
            parameters.object_size > object_config.upload_part_size.max(5 << 20),
            "object-size must exceed configured upload_part_size and 5 MiB to exercise multipart upload"
        );
    }
    let store_from_env = args.store.is_none();
    let store_url = args.store.or_else(|| std::env::var("RW_HUMMOCK_URL").ok())
        .context("provide --store s3://bucket or RW_HUMMOCK_URL; credentials and endpoint use the existing RW environment")?;
    let store_url = store_url.strip_prefix("hummock+").unwrap_or(&store_url);
    ensure!(
        store_url.starts_with("s3://") || store_url.starts_with("minio://"),
        "this benchmark supports S3/MinIO URLs"
    );
    let object_config = Arc::new(object_config);
    let metrics = Arc::new(ObjectStoreMetrics::unused());
    // Use the standard RW backend initialization, including its credential and
    // TLS configuration. Benchmarks do not construct a separate HTTP client.
    let store = build_remote_object_store(
        store_url,
        metrics.clone(),
        "risectl-bench",
        object_config.clone(),
    )
    .await;
    let store = Arc::new(store);
    let prefix = args
        .reuse_prefix
        .unwrap_or_else(|| format!("risectl-bench/{}/", uuid::Uuid::new_v4()));
    eprintln!(
        "benchmark target: {} prefix={prefix}; large dataset={} MiB; one shared {} MiB payload, plus worker/SDK buffers",
        redact_url(store_url),
        parameters.objects as u64 * parameters.object_size as u64 / (1 << 20),
        parameters.object_size / (1 << 20)
    );
    let token = shutdown.child_token();
    let deadline_token = token.clone();
    let deadline = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(args.max_runtime_secs)).await;
        deadline_token.cancel();
    });
    let result = benchmark::run(store, parameters, prefix, token).await;
    deadline.abort();
    let report = result?;
    let succeeded = report.succeeded();
    eprint!("{}", benchmark::terminal_summary(&report));
    let mut json = json!({
        "build": { "version": risingwave_common::RW_VERSION, "git_sha": risingwave_common::GIT_SHA },
        "target": {
            "store": redact_url(store_url),
            "store_source": if store_from_env { "RW_HUMMOCK_URL" } else { "--store" },
            "endpoint_env": std::env::var("RW_S3_ENDPOINT").ok().map(|s| redact_url(&s)),
            "region_env": std::env::var("AWS_REGION").or_else(|_| std::env::var("AWS_DEFAULT_REGION")).ok(),
            "legacy_minio_region": if store_url.starts_with("minio://") { Some("custom") } else { None },
            "path_style_env": std::env::var("RW_IS_FORCE_PATH_STYLE").ok(),
            "backend": if object_config.s3.developer.use_opendal { "opendal" } else { "aws" },
            "config_source": args.config_path,
            "config_defaults_used": args.config_path.is_none(),
            "object_store_config": object_config,
            "abort_on_error_override": true,
            "execution_host": std::env::var("HOSTNAME").ok(),
            "proxy_configured": (["HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy", "ALL_PROXY", "all_proxy"].iter().any(|name| std::env::var_os(name).is_some())),
        },
        "stop_reason": if report.cancelled { if shutdown.is_cancelled() { "shutdown" } else { "deadline" } } else { "finished" },
        "observed_outer_retry_events": benchmark::observed_retries(&metrics),
        "benchmark": report,
    });
    let mut secrets: Vec<String> = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ]
    .iter()
    .filter_map(|key| std::env::var(key).ok())
    .collect();
    if let Some((_, rest)) = store_url.split_once("://")
        && let Some((credentials, _)) = rest.rsplit_once('@')
    {
        secrets.extend(credentials.split(':').map(str::to_owned));
    }
    redact_errors(&mut json, &secrets);
    let text = serde_json::to_string_pretty(&json)?;
    let saved = output
        .map(|(mut file, path)| {
            file.write_all(text.as_bytes())?;
            file.flush()?;
            file.persist(path)
                .map_err(|error| error.error)
                .context("persist benchmark report")?;
            Ok::<_, anyhow::Error>(())
        })
        .transpose();
    // Attempt both outputs before propagating either error. In particular, a
    // late file error must not suppress the complete JSON document on stdout.
    let printed = writeln!(std::io::stdout().lock(), "{text}").context("write report to stdout");
    saved.context("save benchmark report; full JSON was also attempted on stdout")?;
    printed?;
    if !succeeded {
        bail!(
            "benchmark did not complete successfully; inspect the JSON phase and cleanup results"
        );
    }
    Ok(())
}
