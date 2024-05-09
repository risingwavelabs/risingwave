// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::{Context, Error};
use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema, SchemaRef};
use arrow_udf_flight::Client as FlightClient;
use arrow_udf_js::{CallMode as JsCallMode, Runtime as JsRuntime};
#[cfg(feature = "embedded-deno-udf")]
use arrow_udf_js_deno::{CallMode as DenoCallMode, Runtime as DenoRuntime};
#[cfg(feature = "embedded-python-udf")]
use arrow_udf_python::{CallMode as PythonCallMode, Runtime as PythonRuntime};
use arrow_udf_wasm::Runtime as WasmRuntime;
use await_tree::InstrumentAwait;
use cfg_or_panic::cfg_or_panic;
use ginepro::{LoadBalancedChannel, ResolutionStrategy};
use moka::sync::Cache;
use prometheus::{
    exponential_buckets, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, HistogramVec, IntCounter, IntCounterVec, Registry,
};
use risingwave_common::array::arrow::{FromArrow, ToArrow, UdfArrowConvert};
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::addr::HostAddr;
use risingwave_expr::expr_context::FRAGMENT_ID;
use risingwave_pb::expr::ExprNode;
use thiserror_ext::AsReport;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::{bail, Result};

#[derive(Debug)]
pub struct UserDefinedFunction {
    children: Vec<BoxedExpression>,
    arg_types: Vec<DataType>,
    return_type: DataType,
    arg_schema: SchemaRef,
    imp: UdfImpl,
    identifier: String,
    link: Option<String>,
    arrow_convert: UdfArrowConvert,
    span: await_tree::Span,
    /// Number of remaining successful calls until retry is enabled.
    /// This parameter is designed to prevent continuous retry on every call, which would increase delay.
    /// Logic:
    /// It resets to `INITIAL_RETRY_COUNT` after a single failure and then decrements with each call, enabling retry when it reaches zero.
    /// If non-zero, we will not retry on connection errors to prevent blocking the stream.
    /// On each connection error, the count will be reset to `INITIAL_RETRY_COUNT`.
    /// On each successful call, the count will be decreased by 1.
    /// Link:
    /// See <https://github.com/risingwavelabs/risingwave/issues/13791>.
    disable_retry_count: AtomicU8,
    /// Always retry. Overrides `disable_retry_count`.
    always_retry_on_network_error: bool,
}

const INITIAL_RETRY_COUNT: u8 = 16;

#[derive(Debug)]
pub enum UdfImpl {
    External(Arc<FlightClient>),
    Wasm(Arc<WasmRuntime>),
    JavaScript(JsRuntime),
    #[cfg(feature = "embedded-python-udf")]
    Python(PythonRuntime),
    #[cfg(feature = "embedded-deno-udf")]
    Deno(Arc<DenoRuntime>),
}

#[async_trait::async_trait]
impl Expression for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        if input.cardinality() == 0 {
            // early return for empty input
            let mut builder = self.return_type.create_array_builder(input.capacity());
            builder.append_n_null(input.capacity());
            return Ok(builder.finish().into_ref());
        }
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let array = child.eval(input).await?;
            columns.push(array);
        }
        let chunk = DataChunk::new(columns, input.visibility().clone());
        self.eval_inner(&chunk).await
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            columns.push(datum);
        }
        let arg_row = OwnedRow::new(columns);
        let chunk = DataChunk::from_rows(std::slice::from_ref(&arg_row), &self.arg_types);
        let output_array = self.eval_inner(&chunk).await?;
        Ok(output_array.to_datum())
    }
}

impl UserDefinedFunction {
    async fn eval_inner(&self, input: &DataChunk) -> Result<ArrayRef> {
        // this will drop invisible rows
        let arrow_input = self
            .arrow_convert
            .to_record_batch(self.arg_schema.clone(), input)?;

        // metrics
        let metrics = &*GLOBAL_METRICS;
        // batch query does not have a fragment_id
        let fragment_id = FRAGMENT_ID::try_with(ToOwned::to_owned)
            .unwrap_or(0)
            .to_string();
        let language = match &self.imp {
            UdfImpl::Wasm(_) => "wasm",
            UdfImpl::JavaScript(_) => "javascript(quickjs)",
            #[cfg(feature = "embedded-python-udf")]
            UdfImpl::Python(_) => "python",
            #[cfg(feature = "embedded-deno-udf")]
            UdfImpl::Deno(_) => "javascript(deno)",
            UdfImpl::External(_) => "external",
        };
        let labels: &[&str; 4] = &[
            self.link.as_deref().unwrap_or(""),
            language,
            &self.identifier,
            fragment_id.as_str(),
        ];
        metrics
            .udf_input_chunk_rows
            .with_label_values(labels)
            .observe(arrow_input.num_rows() as f64);
        metrics
            .udf_input_rows
            .with_label_values(labels)
            .inc_by(arrow_input.num_rows() as u64);
        metrics
            .udf_input_bytes
            .with_label_values(labels)
            .inc_by(arrow_input.get_array_memory_size() as u64);
        let timer = metrics.udf_latency.with_label_values(labels).start_timer();

        let arrow_output_result: Result<arrow_array::RecordBatch, Error> = match &self.imp {
            UdfImpl::Wasm(runtime) => runtime.call(&self.identifier, &arrow_input),
            UdfImpl::JavaScript(runtime) => runtime.call(&self.identifier, &arrow_input),
            #[cfg(feature = "embedded-python-udf")]
            UdfImpl::Python(runtime) => runtime.call(&self.identifier, &arrow_input),
            #[cfg(feature = "embedded-deno-udf")]
            UdfImpl::Deno(runtime) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(runtime.call(&self.identifier, arrow_input))
            }),
            UdfImpl::External(client) => {
                let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
                let result = if self.always_retry_on_network_error {
                    call_with_always_retry_on_network_error(
                        client,
                        &self.identifier,
                        &arrow_input,
                        &metrics.udf_retry_count.with_label_values(labels),
                    )
                    .instrument_await(self.span.clone())
                    .await
                } else {
                    let result = if disable_retry_count != 0 {
                        client
                            .call(&self.identifier, &arrow_input)
                            .instrument_await(self.span.clone())
                            .await
                    } else {
                        call_with_retry(client, &self.identifier, &arrow_input)
                            .instrument_await(self.span.clone())
                            .await
                    };
                    let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
                    let connection_error = matches!(&result, Err(e) if is_connection_error(e));
                    if connection_error && disable_retry_count != INITIAL_RETRY_COUNT {
                        // reset count on connection error
                        self.disable_retry_count
                            .store(INITIAL_RETRY_COUNT, Ordering::Relaxed);
                    } else if !connection_error && disable_retry_count != 0 {
                        // decrease count on success, ignore if exchange failed
                        _ = self.disable_retry_count.compare_exchange(
                            disable_retry_count,
                            disable_retry_count - 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                    }
                    result
                };
                result.map_err(|e| e.into())
            }
        };
        timer.stop_and_record();
        if arrow_output_result.is_ok() {
            &metrics.udf_success_count
        } else {
            &metrics.udf_failure_count
        }
        .with_label_values(labels)
        .inc();

        let arrow_output = arrow_output_result?;

        if arrow_output.num_rows() != input.cardinality() {
            bail!(
                "UDF returned {} rows, but expected {}",
                arrow_output.num_rows(),
                input.cardinality(),
            );
        }

        let output = self.arrow_convert.from_record_batch(&arrow_output)?;
        let output = output.uncompact(input.visibility().clone());

        let Some(array) = output.columns().first() else {
            bail!("UDF returned no columns");
        };
        if !array.data_type().equals_datatype(&self.return_type) {
            bail!(
                "UDF returned {:?}, but expected {:?}",
                array.data_type(),
                self.return_type,
            );
        }

        Ok(array.clone())
    }
}

/// Call a function, retry up to 5 times / 3s if connection is broken.
async fn call_with_retry(
    client: &FlightClient,
    id: &str,
    input: &RecordBatch,
) -> Result<RecordBatch, arrow_udf_flight::Error> {
    let mut backoff = Duration::from_millis(100);
    for i in 0..5 {
        match client.call(id, input).await {
            Err(err) if is_connection_error(&err) && i != 4 => {
                tracing::error!(error = %err.as_report(), "UDF connection error. retry...");
            }
            ret => return ret,
        }
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }
    unreachable!()
}

/// Always retry on connection error
async fn call_with_always_retry_on_network_error(
    client: &FlightClient,
    id: &str,
    input: &RecordBatch,
    retry_count: &IntCounter,
) -> Result<RecordBatch, arrow_udf_flight::Error> {
    let mut backoff = Duration::from_millis(100);
    loop {
        match client.call(id, input).await {
            Err(err) if is_tonic_error(&err) => {
                tracing::error!(error = %err.as_report(), "UDF tonic error. retry...");
            }
            ret => {
                if ret.is_err() {
                    tracing::error!(error = %ret.as_ref().unwrap_err().as_report(), "UDF error. exiting...");
                }
                return ret;
            }
        }
        retry_count.inc();
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }
}

impl Build for UserDefinedFunction {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let udf = prost.get_rex_node().unwrap().as_udf().unwrap();
        let mut arrow_convert = UdfArrowConvert::default();

        #[cfg(not(feature = "embedded-deno-udf"))]
        let runtime = "quickjs";

        #[cfg(feature = "embedded-deno-udf")]
        let runtime = match udf.runtime.as_deref() {
            Some("deno") => "deno",
            _ => "quickjs",
        };

        let identifier = udf.get_identifier()?;
        let imp = match udf.language.as_str() {
            #[cfg(not(madsim))]
            "wasm" | "rust" => {
                let compressed_wasm_binary = udf.get_compressed_binary()?;
                let wasm_binary = zstd::stream::decode_all(compressed_wasm_binary.as_slice())
                    .context("failed to decompress wasm binary")?;
                let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
                // backward compatibility
                // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
                if runtime.abi_version().0 <= 2 {
                    arrow_convert = UdfArrowConvert { legacy: true };
                }
                UdfImpl::Wasm(runtime)
            }
            "javascript" if runtime != "deno" => {
                let mut rt = JsRuntime::new()?;
                let body = format!(
                    "export function {}({}) {{ {} }}",
                    identifier,
                    udf.arg_names.join(","),
                    udf.get_body()?
                );
                rt.add_function(
                    identifier,
                    arrow_convert.to_arrow_field("", &return_type)?,
                    JsCallMode::CalledOnNullInput,
                    &body,
                )?;
                UdfImpl::JavaScript(rt)
            }
            #[cfg(feature = "embedded-deno-udf")]
            "javascript" if runtime == "deno" => {
                let rt = DenoRuntime::new();
                let body = match udf.get_body() {
                    Ok(body) => body.clone(),
                    Err(_) => match udf.get_compressed_binary() {
                        Ok(compressed_binary) => {
                            let binary = zstd::stream::decode_all(compressed_binary.as_slice())
                                .context("failed to decompress binary")?;
                            String::from_utf8(binary).context("failed to decode binary")?
                        }
                        Err(_) => {
                            bail!("UDF body or compressed binary is required for deno UDF");
                        }
                    },
                };

                let body = if matches!(udf.function_type.as_deref(), Some("async")) {
                    format!(
                        "export async function {}({}) {{ {} }}",
                        identifier,
                        udf.arg_names.join(","),
                        body
                    )
                } else {
                    format!(
                        "export function {}({}) {{ {} }}",
                        identifier,
                        udf.arg_names.join(","),
                        body
                    )
                };

                futures::executor::block_on(rt.add_function(
                    identifier,
                    arrow_convert.to_arrow_field("", &return_type)?,
                    DenoCallMode::CalledOnNullInput,
                    &body,
                ))?;

                UdfImpl::Deno(rt)
            }
            #[cfg(feature = "embedded-python-udf")]
            "python" if udf.body.is_some() => {
                let mut rt = PythonRuntime::builder().sandboxed(true).build()?;
                let body = udf.get_body()?;
                rt.add_function(
                    identifier,
                    arrow_convert.to_arrow_field("", &return_type)?,
                    PythonCallMode::CalledOnNullInput,
                    body,
                )?;
                UdfImpl::Python(rt)
            }
            #[cfg(not(madsim))]
            _ => {
                let link = udf.get_link()?;
                let client = crate::expr::expr_udf::get_or_create_flight_client(link)?;
                // backward compatibility
                // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
                if client.protocol_version() == 1 {
                    arrow_convert = UdfArrowConvert { legacy: true };
                }
                UdfImpl::External(client)
            }
            #[cfg(madsim)]
            l => panic!("UDF language {l:?} is not supported on madsim"),
        };

        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map(|t| arrow_convert.to_arrow_field("", &DataType::from(t)))
                .try_collect::<Fields>()?,
        ));

        Ok(Self {
            children: udf.children.iter().map(build_child).try_collect()?,
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            arg_schema,
            imp,
            identifier: identifier.clone(),
            link: udf.link.clone(),
            arrow_convert,
            span: format!("udf_call({})", identifier).into(),
            disable_retry_count: AtomicU8::new(0),
            always_retry_on_network_error: udf.always_retry_on_network_error,
        })
    }
}

#[cfg(not(madsim))]
/// Get or create a client for the given UDF service.
///
/// There is a global cache for clients, so that we can reuse the same client for the same service.
pub(crate) fn get_or_create_flight_client(link: &str) -> Result<Arc<FlightClient>> {
    static CLIENTS: LazyLock<std::sync::Mutex<HashMap<String, Weak<FlightClient>>>> =
        LazyLock::new(Default::default);
    let mut clients = CLIENTS.lock().unwrap();
    if let Some(client) = clients.get(link).and_then(|c| c.upgrade()) {
        // reuse existing client
        Ok(client)
    } else {
        // create new client
        let client = Arc::new(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let channel = connect_tonic(link).await?;
                Ok(FlightClient::new(channel).await?) as Result<_>
            })
        })?);
        clients.insert(link.to_owned(), Arc::downgrade(&client));
        Ok(client)
    }
}

/// Connect to a UDF service and return a tonic `Channel`.
async fn connect_tonic(mut addr: &str) -> Result<tonic::transport::Channel> {
    // Interval between two successive probes of the UDF DNS.
    const DNS_PROBE_INTERVAL_SECS: u64 = 5;
    // Timeout duration for performing an eager DNS resolution.
    const EAGER_DNS_RESOLVE_TIMEOUT_SECS: u64 = 5;
    const REQUEST_TIMEOUT_SECS: u64 = 5;
    const CONNECT_TIMEOUT_SECS: u64 = 5;

    if addr.starts_with("http://") {
        addr = addr.strip_prefix("http://").unwrap();
    }
    if addr.starts_with("https://") {
        addr = addr.strip_prefix("https://").unwrap();
    }
    let host_addr = addr.parse::<HostAddr>().map_err(|e| {
        arrow_udf_flight::Error::Service(format!(
            "invalid address: {}, err: {}",
            addr,
            e.as_report()
        ))
    })?;
    let channel = LoadBalancedChannel::builder((host_addr.host.clone(), host_addr.port))
        .dns_probe_interval(std::time::Duration::from_secs(DNS_PROBE_INTERVAL_SECS))
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .resolution_strategy(ResolutionStrategy::Eager {
            timeout: tokio::time::Duration::from_secs(EAGER_DNS_RESOLVE_TIMEOUT_SECS),
        })
        .channel()
        .await
        .map_err(|e| {
            arrow_udf_flight::Error::Service(format!(
                "failed to create LoadBalancedChannel, address: {}, err: {}",
                host_addr,
                e.as_report()
            ))
        })?;
    Ok(channel.into())
}

/// Get or create a wasm runtime.
///
/// Runtimes returned by this function are cached inside for at least 60 seconds.
/// Later calls with the same binary will reuse the same runtime.
#[cfg_or_panic(not(madsim))]
pub fn get_or_create_wasm_runtime(binary: &[u8]) -> Result<Arc<WasmRuntime>> {
    static RUNTIMES: LazyLock<Cache<md5::Digest, Arc<WasmRuntime>>> = LazyLock::new(|| {
        Cache::builder()
            .time_to_idle(Duration::from_secs(60))
            .build()
    });

    let md5 = md5::compute(binary);
    if let Some(runtime) = RUNTIMES.get(&md5) {
        return Ok(runtime.clone());
    }

    let runtime = Arc::new(arrow_udf_wasm::Runtime::new(binary)?);
    RUNTIMES.insert(md5, runtime.clone());
    Ok(runtime)
}

/// Returns true if the arrow flight error is caused by a connection error.
fn is_connection_error(err: &arrow_udf_flight::Error) -> bool {
    match err {
        // Connection refused
        arrow_udf_flight::Error::Tonic(status) if status.code() == tonic::Code::Unavailable => true,
        _ => false,
    }
}

fn is_tonic_error(err: &arrow_udf_flight::Error) -> bool {
    matches!(
        err,
        arrow_udf_flight::Error::Tonic(_)
            | arrow_udf_flight::Error::Flight(arrow_flight::error::FlightError::Tonic(_))
    )
}

/// Monitor metrics for UDF.
#[derive(Debug, Clone)]
struct Metrics {
    /// Number of successful UDF calls.
    udf_success_count: IntCounterVec,
    /// Number of failed UDF calls.
    udf_failure_count: IntCounterVec,
    /// Total number of retried UDF calls.
    udf_retry_count: IntCounterVec,
    /// Input chunk rows of UDF calls.
    udf_input_chunk_rows: HistogramVec,
    /// The latency of UDF calls in seconds.
    udf_latency: HistogramVec,
    /// Total number of input rows of UDF calls.
    udf_input_rows: IntCounterVec,
    /// Total number of input bytes of UDF calls.
    udf_input_bytes: IntCounterVec,
}

/// Global UDF metrics.
static GLOBAL_METRICS: LazyLock<Metrics> = LazyLock::new(|| Metrics::new(&GLOBAL_METRICS_REGISTRY));

impl Metrics {
    fn new(registry: &Registry) -> Self {
        let labels = &["link", "language", "name", "fragment_id"];
        let udf_success_count = register_int_counter_vec_with_registry!(
            "udf_success_count",
            "Total number of successful UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_failure_count = register_int_counter_vec_with_registry!(
            "udf_failure_count",
            "Total number of failed UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_retry_count = register_int_counter_vec_with_registry!(
            "udf_retry_count",
            "Total number of retried UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_input_chunk_rows = register_histogram_vec_with_registry!(
            "udf_input_chunk_rows",
            "Input chunk rows of UDF calls",
            labels,
            exponential_buckets(1.0, 2.0, 10).unwrap(), // 1 to 1024
            registry
        )
        .unwrap();
        let udf_latency = register_histogram_vec_with_registry!(
            "udf_latency",
            "The latency(s) of UDF calls",
            labels,
            exponential_buckets(0.000001, 2.0, 30).unwrap(), // 1us to 1000s
            registry
        )
        .unwrap();
        let udf_input_rows = register_int_counter_vec_with_registry!(
            "udf_input_rows",
            "Total number of input rows of UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_input_bytes = register_int_counter_vec_with_registry!(
            "udf_input_bytes",
            "Total number of input bytes of UDF calls",
            labels,
            registry
        )
        .unwrap();

        Metrics {
            udf_success_count,
            udf_failure_count,
            udf_retry_count,
            udf_input_chunk_rows,
            udf_latency,
            udf_input_rows,
            udf_input_bytes,
        }
    }
}
