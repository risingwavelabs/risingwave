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
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::{Context, Error};
use arrow_schema::{Field, Fields, Schema};
use arrow_udf_js::{CallMode as JsCallMode, Runtime as JsRuntime};
#[cfg(feature = "embedded-python-udf")]
use arrow_udf_python::{CallMode as PythonCallMode, Runtime as PythonRuntime};
use arrow_udf_wasm::Runtime as WasmRuntime;
use await_tree::InstrumentAwait;
use cfg_or_panic::cfg_or_panic;
use moka::sync::Cache;
use risingwave_common::array::{ArrayError, ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr_context::FRAGMENT_ID;
use risingwave_pb::expr::ExprNode;
use risingwave_udf::metrics::GLOBAL_METRICS;
use risingwave_udf::ArrowFlightUdfClient;
use thiserror_ext::AsReport;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::{bail, Result};

#[derive(Debug)]
pub struct UserDefinedFunction {
    children: Vec<BoxedExpression>,
    arg_types: Vec<DataType>,
    return_type: DataType,
    #[expect(dead_code)]
    arg_schema: Arc<Schema>,
    imp: UdfImpl,
    identifier: String,
    span: await_tree::Span,
    /// Number of remaining successful calls until retry is enabled.
    /// If non-zero, we will not retry on connection errors to prevent blocking the stream.
    /// On each connection error, the count will be reset to `INITIAL_RETRY_COUNT`.
    /// On each successful call, the count will be decreased by 1.
    /// See <https://github.com/risingwavelabs/risingwave/issues/13791>.
    disable_retry_count: AtomicU8,
    /// Always retry. Overrides `disable_retry_count`.
    always_retry_on_network_error: bool,
}

const INITIAL_RETRY_COUNT: u8 = 16;

#[derive(Debug)]
pub enum UdfImpl {
    External(Arc<ArrowFlightUdfClient>),
    Wasm(Arc<WasmRuntime>),
    JavaScript(JsRuntime),
    #[cfg(feature = "embedded-python-udf")]
    Python(PythonRuntime),
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
        let arrow_input = arrow_array::RecordBatch::try_from(input)?;

        // metrics
        let metrics = &*GLOBAL_METRICS;
        // batch query does not have a fragment_id
        let fragment_id = FRAGMENT_ID::try_with(ToOwned::to_owned)
            .unwrap_or(0)
            .to_string();
        let addr = match &self.imp {
            UdfImpl::External(client) => client.get_addr(),
            _ => "",
        };
        let language = match &self.imp {
            UdfImpl::Wasm(_) => "wasm",
            UdfImpl::JavaScript(_) => "javascript",
            #[cfg(feature = "embedded-python-udf")]
            UdfImpl::Python(_) => "python",
            UdfImpl::External(_) => "external",
        };
        let labels: &[&str; 4] = &[addr, language, &self.identifier, fragment_id.as_str()];
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
            UdfImpl::External(client) => {
                let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
                let result = if self.always_retry_on_network_error {
                    client
                        .call_with_always_retry_on_network_error(&self.identifier, arrow_input)
                        .instrument_await(self.span.clone())
                        .await
                } else {
                    let result = if disable_retry_count != 0 {
                        client
                            .call(&self.identifier, arrow_input)
                            .instrument_await(self.span.clone())
                            .await
                    } else {
                        client
                            .call_with_retry(&self.identifier, arrow_input)
                            .instrument_await(self.span.clone())
                            .await
                    };
                    let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
                    let connection_error = matches!(&result, Err(e) if e.is_connection_error());
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

        let output = DataChunk::try_from(&arrow_output)?;
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

impl Build for UserDefinedFunction {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let udf = prost.get_rex_node().unwrap().as_udf().unwrap();

        let identifier = udf.get_identifier()?;
        let imp = match udf.language.as_str() {
            #[cfg(not(madsim))]
            "wasm" | "rust" => {
                let compressed_wasm_binary = udf.get_compressed_binary()?;
                let wasm_binary = zstd::stream::decode_all(compressed_wasm_binary.as_slice())
                    .context("failed to decompress wasm binary")?;
                let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
                UdfImpl::Wasm(runtime)
            }
            "javascript" => {
                let mut rt = JsRuntime::new()?;
                let body = format!(
                    "export function {}({}) {{ {} }}",
                    identifier,
                    udf.arg_names.join(","),
                    udf.get_body()?
                );
                rt.add_function(
                    identifier,
                    arrow_schema::DataType::try_from(&return_type)?,
                    JsCallMode::CalledOnNullInput,
                    &body,
                )?;
                UdfImpl::JavaScript(rt)
            }
            #[cfg(feature = "embedded-python-udf")]
            "python" if udf.body.is_some() => {
                let mut rt = PythonRuntime::builder().sandboxed(true).build()?;
                let body = udf.get_body()?;
                rt.add_function(
                    identifier,
                    arrow_schema::DataType::try_from(&return_type)?,
                    PythonCallMode::CalledOnNullInput,
                    body,
                )?;
                UdfImpl::Python(rt)
            }
            #[cfg(not(madsim))]
            _ => {
                let link = udf.get_link()?;
                UdfImpl::External(get_or_create_flight_client(link)?)
            }
            #[cfg(madsim)]
            l => panic!("UDF language {l:?} is not supported on madsim"),
        };

        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map::<Result<_>, _>(|t| {
                    Ok(Field::new(
                        "",
                        DataType::from(t).try_into().map_err(|e: ArrayError| {
                            risingwave_udf::Error::unsupported(e.to_report_string())
                        })?,
                        true,
                    ))
                })
                .try_collect::<Fields>()?,
        ));

        Ok(Self {
            children: udf.children.iter().map(build_child).try_collect()?,
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            arg_schema,
            imp,
            identifier: identifier.clone(),
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
pub(crate) fn get_or_create_flight_client(link: &str) -> Result<Arc<ArrowFlightUdfClient>> {
    static CLIENTS: LazyLock<std::sync::Mutex<HashMap<String, Weak<ArrowFlightUdfClient>>>> =
        LazyLock::new(Default::default);
    let mut clients = CLIENTS.lock().unwrap();
    if let Some(client) = clients.get(link).and_then(|c| c.upgrade()) {
        // reuse existing client
        Ok(client)
    } else {
        // create new client
        let client = Arc::new(ArrowFlightUdfClient::connect_lazy(link)?);
        clients.insert(link.into(), Arc::downgrade(&client));
        Ok(client)
    }
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
