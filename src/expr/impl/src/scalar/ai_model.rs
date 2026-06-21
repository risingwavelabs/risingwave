// Copyright 2025 RisingWave Labs
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

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::embeddings::{CreateEmbeddingRequestArgs, Embedding, EmbeddingInput};
use prometheus::{Registry, exponential_buckets};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, F32Array, ListArrayBuilder, ListValue,
};
use risingwave_common::metrics::*;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, F32, ScalarImpl};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{ExprError, Result, build_function};
use serde::Deserialize;
use serde_json::Value;
use thiserror_ext::AsReport;

const OPENAI_EMBEDDING_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const OPENAI_EMBEDDING_RETRY_MAX_DELAY: Duration = Duration::from_secs(10);
const OPENAI_EMBEDDING_SLOW_REQUEST_THRESHOLD: Duration = Duration::from_secs(60);

/// `OpenAI` embedding context that holds the client and model configuration
#[derive(Debug)]
pub struct OpenAiEmbeddingContext {
    pub client: Client<OpenAIConfig>,
    pub model: String,
}

#[derive(Deserialize)]
struct OpenAiEmbeddingConfig {
    model: String,
    api_key: Option<String>,
    org_id: Option<String>,
    project_id: Option<String>,
    api_base: Option<String>,
}

impl OpenAiEmbeddingContext {
    /// Create a new `OpenAI` embedding context from `api_key` and model
    pub fn from_config(config: Value) -> Result<Self> {
        let param: OpenAiEmbeddingConfig = serde_json::from_value(config).map_err(|err| {
            invalid_param_err(format!("failed to parse config: {}", err.as_report()))
        })?;

        let mut config = OpenAIConfig::new();
        if let Some(api_key) = param.api_key {
            config = config.with_api_key(api_key);
        }
        if let Some(org_id) = param.org_id {
            config = config.with_org_id(org_id);
        }
        if let Some(proj_id) = param.project_id {
            config = config.with_project_id(proj_id);
        }
        if let Some(api_base) = param.api_base {
            config = config.with_api_base(api_base);
        }

        let client = Client::with_config(config);
        Ok(Self {
            client,
            model: param.model,
        })
    }
}

#[derive(Debug)]
struct OpenAiEmbedding {
    text_expr: BoxedExpression,
    context: OpenAiEmbeddingContext,
    metrics: OpenAiEmbeddingMetrics,
}

impl OpenAiEmbedding {
    async fn get_embeddings(
        &self,
        input: EmbeddingInput,
        word_counts: &[usize],
    ) -> Result<Vec<Embedding>> {
        let expected_embedding_count = word_counts.len();
        let request_start = Instant::now();

        self.metrics
            .input_rows
            .inc_by(expected_embedding_count as u64);
        for &word_count in word_counts {
            self.metrics.input_row_word_count.observe(word_count as f64);
        }

        let request = CreateEmbeddingRequestArgs::default()
            .model(&self.context.model)
            .input(input)
            .build()
            .map_err(|e| {
                self.metrics.failure_count.inc();
                tracing::error!(error = %e.as_report(), "Failed to build OpenAI embedding request");
                ExprError::Custom("failed to build OpenAI embedding request".into())
            })?;

        let mut backoff = OPENAI_EMBEDDING_RETRY_BASE_DELAY;
        loop {
            let timer = self.metrics.latency.start_timer();
            let result = {
                let _inflight_guard = OpenAiEmbeddingInflightGuard::new(
                    &self.metrics.inflight_requests,
                    &self.metrics.inflight_rows,
                    expected_embedding_count,
                );
                self.context
                    .client
                    .embeddings()
                    .create(request.clone())
                    .await
                    .map_err(|e| {
                        ExprError::Custom(format!(
                            "failed to get embedding from OpenAI: {}",
                            e.as_report()
                        ))
                    })
                    .and_then(|response| {
                        if response.data.len() != expected_embedding_count {
                            return Err(ExprError::Custom(
                                "number of embeddings returned from OpenAI does not match the number of texts"
                                    .into(),
                            ));
                        }
                        Ok(response.data)
                    })
            };

            match result {
                Ok(embeddings) => {
                    timer.stop_and_record();
                    let request_latency = request_start.elapsed();
                    if request_latency > OPENAI_EMBEDDING_SLOW_REQUEST_THRESHOLD {
                        let word_count_stats = WordCountStats::new(word_counts);
                        tracing::warn!(
                            model = %self.context.model,
                            row_count = expected_embedding_count,
                            word_count_min = word_count_stats.min,
                            word_count_avg = word_count_stats.avg,
                            word_count_p50 = word_count_stats.p50,
                            word_count_p99 = word_count_stats.p99,
                            word_count_max = word_count_stats.max,
                            latency_ms = request_latency.as_millis(),
                            latency_secs = request_latency.as_secs_f64(),
                            "Slow OpenAI embedding request finished"
                        );
                    }
                    self.metrics.success_count.inc();
                    return Ok(embeddings);
                }
                Err(err) => {
                    timer.stop_and_discard();
                    self.metrics.failure_count.inc();
                    tracing::error!(
                        ?backoff,
                        error = %err.as_report(),
                        "Failed to get embedding from OpenAI, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(OPENAI_EMBEDDING_RETRY_MAX_DELAY);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct WordCountStats {
    min: usize,
    avg: f64,
    p50: usize,
    p99: usize,
    max: usize,
}

impl WordCountStats {
    fn new(word_counts: &[usize]) -> Self {
        if word_counts.is_empty() {
            return Self {
                min: 0,
                avg: 0.0,
                p50: 0,
                p99: 0,
                max: 0,
            };
        }

        let mut sorted = word_counts.to_vec();
        sorted.sort_unstable();
        let total: usize = sorted.iter().sum();

        Self {
            min: sorted[0],
            avg: total as f64 / sorted.len() as f64,
            p50: percentile_nearest_rank(&sorted, 0.50),
            p99: percentile_nearest_rank(&sorted, 0.99),
            max: sorted[sorted.len() - 1],
        }
    }
}

fn percentile_nearest_rank(sorted: &[usize], percentile: f64) -> usize {
    debug_assert!(!sorted.is_empty());
    let rank = (sorted.len() as f64 * percentile).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().count()
}

struct OpenAiEmbeddingInflightGuard {
    inflight_requests: LabelGuardedIntGauge,
    inflight_rows: LabelGuardedIntGauge,
    row_count: i64,
}

impl OpenAiEmbeddingInflightGuard {
    fn new(
        inflight_requests: &LabelGuardedIntGauge,
        inflight_rows: &LabelGuardedIntGauge,
        row_count: usize,
    ) -> Self {
        let row_count = row_count.try_into().unwrap_or(i64::MAX);
        inflight_requests.inc();
        inflight_rows.add(row_count);
        Self {
            inflight_requests: inflight_requests.clone(),
            inflight_rows: inflight_rows.clone(),
            row_count,
        }
    }
}

impl Drop for OpenAiEmbeddingInflightGuard {
    fn drop(&mut self) {
        self.inflight_requests.dec();
        self.inflight_rows.sub(self.row_count);
    }
}

#[async_trait::async_trait]
impl Expression for OpenAiEmbedding {
    fn return_type(&self) -> DataType {
        DataType::Float32.list()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let text_array = self.text_expr.eval(input).await?;
        let text_array = text_array.as_utf8();

        // Collect non-null and non-empty texts
        let mut texts_to_embed = Vec::new();
        let mut word_counts = Vec::new();

        for i in 0..input.capacity() {
            if let Some(text) = text_array.value_at(i)
                && !text.is_empty()
            {
                word_counts.push(count_words(text));
                texts_to_embed.push(text.to_owned());
            }
        }

        // Get embeddings in batch
        let embeddings = if texts_to_embed.is_empty() {
            Vec::new()
        } else {
            self.get_embeddings(EmbeddingInput::StringArray(texts_to_embed), &word_counts)
                .await?
        };

        // Map results back to original positions
        let mut builder = ListArrayBuilder::with_type(input.capacity(), DataType::Float32.list());
        let mut embedding_idx = 0;

        for i in 0..input.capacity() {
            if let Some(text) = text_array.value_at(i) {
                if !text.is_empty() {
                    // Non-empty text, use the embedding result
                    if embedding_idx < embeddings.len() {
                        let embedding = &embeddings[embedding_idx].embedding;
                        let float_array =
                            F32Array::from_iter(embedding.iter().map(|&v| Some(F32::from(v))));
                        let list_value = ListValue::new(float_array.into());
                        builder.append_owned(Some(list_value));
                        embedding_idx += 1;
                    } else {
                        builder.append(None);
                    }
                } else {
                    // Empty text returns NULL
                    builder.append(None);
                }
            } else {
                // Null text returns NULL
                builder.append(None);
            }
        }

        Ok(Arc::new(ArrayImpl::List(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let text_datum = self.text_expr.eval_row(input).await?;

        if let Some(ScalarImpl::Utf8(text)) = text_datum.as_ref() {
            if text.is_empty() {
                return Ok(None);
            }

            let word_count = count_words(text);
            let word_counts = [word_count];
            let embeddings = self
                .get_embeddings(
                    EmbeddingInput::String(text.to_owned().into_string()),
                    &word_counts,
                )
                .await?;
            let embedding = &embeddings[0].embedding;
            let float_array = F32Array::from_iter(embedding.iter().map(|&v| Some(F32::from(v))));
            Ok(Some(ListValue::new(float_array.into()).into()))
        } else {
            Ok(None)
        }
    }
}

fn invalid_param_err(reason: impl Into<String>) -> ExprError {
    ExprError::InvalidParam {
        name: "openai_embedding",
        reason: reason.into().into(),
    }
}

#[build_function("openai_embedding(jsonb, varchar) -> float4[]")]
fn build_openai_embedding_expr(
    _: DataType,
    mut children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    if children.len() != 2 {
        return Err(invalid_param_err("expected 2 arguments"));
    }

    // Check if the first two parameters are constants
    let config = if let Ok(Some(config_scalar)) = children[0].eval_const() {
        if let ScalarImpl::Jsonb(config) = config_scalar {
            config.take()
        } else {
            return Err(invalid_param_err(
                "`embedding_config` must be a jsonb constant",
            ));
        }
    } else {
        return Err(invalid_param_err("`embedding_config` must be a constant"));
    };

    let context = OpenAiEmbeddingContext::from_config(config)?;

    Ok(Box::new(OpenAiEmbedding {
        text_expr: children.pop().unwrap(), // Take the second expression
        metrics: GLOBAL_OPENAI_EMBEDDING_METRICS.with_label_values(&context.model),
        context,
    }))
}

/// Monitor metrics for `openai_embedding`.
#[derive(Debug, Clone)]
struct OpenAiEmbeddingMetricsVec {
    /// Number of successful `OpenAI` embedding requests.
    success_count: LabelGuardedIntCounterVec,
    /// Number of failed `OpenAI` embedding requests.
    failure_count: LabelGuardedIntCounterVec,
    /// The latency of `OpenAI` embedding requests in seconds.
    latency: LabelGuardedHistogramVec,
    /// Total number of non-null, non-empty input rows submitted to `OpenAI` embedding requests.
    input_rows: LabelGuardedIntCounterVec,
    /// Per-row word count of non-null, non-empty input rows submitted to `OpenAI` embedding requests.
    input_row_word_count: LabelGuardedHistogramVec,
    /// Number of in-flight `OpenAI` embedding requests.
    inflight_requests: LabelGuardedIntGaugeVec,
    /// Number of input rows in in-flight `OpenAI` embedding requests.
    inflight_rows: LabelGuardedIntGaugeVec,
}

/// Monitor metrics for `openai_embedding`.
#[derive(Debug, Clone)]
struct OpenAiEmbeddingMetrics {
    /// Number of successful `OpenAI` embedding requests.
    success_count: LabelGuardedIntCounter,
    /// Number of failed `OpenAI` embedding requests.
    failure_count: LabelGuardedIntCounter,
    /// The latency of `OpenAI` embedding requests in seconds.
    latency: LabelGuardedHistogram,
    /// Total number of non-null, non-empty input rows submitted to `OpenAI` embedding requests.
    input_rows: LabelGuardedIntCounter,
    /// Per-row word count of non-null, non-empty input rows submitted to `OpenAI` embedding requests.
    input_row_word_count: LabelGuardedHistogram,
    /// Number of in-flight `OpenAI` embedding requests.
    inflight_requests: LabelGuardedIntGauge,
    /// Number of input rows in in-flight `OpenAI` embedding requests.
    inflight_rows: LabelGuardedIntGauge,
}

/// Global `openai_embedding` metrics.
static GLOBAL_OPENAI_EMBEDDING_METRICS: LazyLock<OpenAiEmbeddingMetricsVec> =
    LazyLock::new(|| OpenAiEmbeddingMetricsVec::new(&GLOBAL_METRICS_REGISTRY));

impl OpenAiEmbeddingMetricsVec {
    fn new(registry: &Registry) -> Self {
        let labels = &["model"];
        let success_count = register_guarded_int_counter_vec_with_registry!(
            "openai_embedding_success_count",
            "Total number of successful OpenAI embedding requests",
            labels,
            registry
        )
        .unwrap();
        let failure_count = register_guarded_int_counter_vec_with_registry!(
            "openai_embedding_failure_count",
            "Total number of failed OpenAI embedding requests",
            labels,
            registry
        )
        .unwrap();
        let latency = register_guarded_histogram_vec_with_registry!(
            "openai_embedding_latency",
            "The latency(s) of OpenAI embedding requests",
            labels,
            exponential_buckets(0.000001, 2.0, 30).unwrap(), // 1us to 1000s
            registry
        )
        .unwrap();
        let input_rows = register_guarded_int_counter_vec_with_registry!(
            "openai_embedding_input_rows",
            "Total number of non-null, non-empty input rows submitted to OpenAI embedding requests",
            labels,
            registry
        )
        .unwrap();
        let input_row_word_count = register_guarded_histogram_vec_with_registry!(
            "openai_embedding_input_row_word_count",
            "Per-row word count of non-null, non-empty input rows submitted to OpenAI embedding requests",
            labels,
            exponential_buckets(1.0, 2.0, 13).unwrap(), // 1 to 4096 words
            registry
        )
        .unwrap();
        let inflight_requests = register_guarded_int_gauge_vec_with_registry!(
            "openai_embedding_inflight_requests",
            "Number of in-flight OpenAI embedding requests",
            labels,
            registry
        )
        .unwrap();
        let inflight_rows = register_guarded_int_gauge_vec_with_registry!(
            "openai_embedding_inflight_rows",
            "Number of input rows in in-flight OpenAI embedding requests",
            labels,
            registry
        )
        .unwrap();

        Self {
            success_count,
            failure_count,
            latency,
            input_rows,
            input_row_word_count,
            inflight_requests,
            inflight_rows,
        }
    }

    fn with_label_values(&self, model: &str) -> OpenAiEmbeddingMetrics {
        let labels = &[model];

        OpenAiEmbeddingMetrics {
            success_count: self.success_count.with_guarded_label_values(labels),
            failure_count: self.failure_count.with_guarded_label_values(labels),
            latency: self.latency.with_guarded_label_values(labels),
            input_rows: self.input_rows.with_guarded_label_values(labels),
            input_row_word_count: self.input_row_word_count.with_guarded_label_values(labels),
            inflight_requests: self.inflight_requests.with_guarded_label_values(labels),
            inflight_rows: self.inflight_rows.with_guarded_label_values(labels),
        }
    }
}
