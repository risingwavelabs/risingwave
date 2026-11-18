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

//! Shared HTTP inference execution, independent of SQL argument and result types.

use std::sync::LazyLock;
use std::time::Duration;

use reqwest::Client;
use reqwest::header::{CONTENT_TYPE, HeaderMap};
// DECIDE and EXTRACT already share this config type. Keep its wire/session names.
use risingwave_common::config::AiDecideConfig as InferenceConfig;
use risingwave_common::log::LogSuppressor;
use risingwave_common::types::JsonbVal;
use risingwave_expr::{ExprError, Result};
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;

/// A fully encoded request; deliberately has no Debug implementation (credentials).
pub(super) struct PreparedRequest(reqwest::Request);

/// One client per expression instance; its permits span concurrent chunks and rows.
pub(super) struct InferenceClient {
    name: &'static str,
    pub(super) client: Client,
    pub(super) config: InferenceConfig,
    pub(super) request_permits: Option<Semaphore>,
}

impl InferenceClient {
    pub(super) fn new(name: &'static str, config: InferenceConfig) -> Result<Self> {
        config
            .validate()
            .map_err(|reason| ExprError::InvalidParam {
                name,
                reason: reason.replace("ai_decide", name).into(),
            })?;
        if config.max_concurrency as usize > Semaphore::MAX_PERMITS {
            return Err(ExprError::InvalidParam {
                name,
                reason: "max_concurrency exceeds semaphore capacity".into(),
            });
        }
        let request_permits =
            (config.max_concurrency != 0).then(|| Semaphore::new(config.max_concurrency as usize));
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms.into()))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ExprError::Custom(format!("{name}: could not initialize HTTP client")))?;
        Ok(Self {
            name,
            client,
            config,
            request_permits,
        })
    }

    fn error(&self, message: impl std::fmt::Display) -> ExprError {
        // Never include URLs, credentials, arbitrary headers or request/response bodies.
        ExprError::Custom(format!("{}: {message}", self.name))
    }

    /// Build one replayable JSON POST without interpreting the function's payload.
    pub(super) fn prepare_post(
        &self,
        url: &str,
        api_key: &str,
        headers: HeaderMap,
        encode: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<PreparedRequest> {
        let url = reqwest::Url::parse(url).map_err(|_| self.error("invalid model_url"))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(self.error("model_url must be an HTTP(S) URL without embedded credentials"));
        }
        let has_content_type = headers.contains_key(CONTENT_TYPE);
        let mut builder = self.client.post(url).bearer_auth(api_key).headers(headers);
        let body = encode()?;
        // Match RequestBuilder::json: retain an explicitly supplied content type.
        if !has_content_type {
            builder = builder.header(CONTENT_TYPE, "application/json");
        }
        builder
            .body(body)
            .build()
            .map(PreparedRequest)
            .map_err(|_| self.error("could not build HTTP request"))
    }

    /// Decode and validate inside the retry loop, including successful HTTP responses.
    /// Prepare once after admission: waiting rows must not allocate serialized payloads.
    /// Local preparation errors escape; all remote failures retry until cancellation.
    /// The decoder must return sanitized errors, since failures are logged here.
    pub(super) async fn infer<T>(
        &self,
        prepare: impl FnOnce() -> Result<PreparedRequest>,
        decode: impl Fn(JsonbVal) -> Result<T>,
    ) -> Result<T> {
        // Keep the logical request's permit through retries/backoff. Drop releases it
        // on success, local failure, or cancellation, without detached HTTP tasks.
        let _permit = match &self.request_permits {
            Some(permits) => Some(
                permits
                    .acquire()
                    .await
                    .expect("request semaphore is never closed"),
            ),
            None => None,
        };
        let request = prepare()?;
        let mut backoff_ms = self
            .config
            .retry_delay_ms
            .min(self.config.max_retry_delay_ms);
        loop {
            let mut delay = Duration::from_millis(backoff_ms.into());
            // Drop the response and its bytes before sleeping. Retry the same serialized
            // request for every remote failure, including invalid success responses.
            let result = async {
                let mut response = self
                    .client
                    .execute(request.0.try_clone().expect("JSON body is cloneable"))
                    .await
                    .map_err(|err| {
                        self.error(if err.is_timeout() {
                            "HTTP request timed out"
                        } else {
                            "HTTP transport failed"
                        })
                    })?;
                let status = response.status();
                if !status.is_success() {
                    if let Some(seconds) = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                    {
                        delay = Duration::from_secs(seconds)
                            .min(Duration::from_millis(self.config.max_retry_after_ms.into()));
                    }
                    return Err(self.error(format!("HTTP {status}")));
                }
                let too_large = || {
                    self.error(format!(
                        "response exceeds {} bytes",
                        self.config.max_response_bytes
                    ))
                };
                if response
                    .content_length()
                    .is_some_and(|len| len > u64::from(self.config.max_response_bytes))
                {
                    return Err(too_large());
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| self.error("HTTP response body read failed"))?
                {
                    if chunk.len()
                        > (self.config.max_response_bytes as usize).saturating_sub(bytes.len())
                    {
                        return Err(too_large());
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let json = jsonbb::Value::from_text(&bytes)
                    .map(JsonbVal::from)
                    .map_err(|_| self.error("successful response is not valid JSON"))?;
                decode(json)
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                        LazyLock::new(LogSuppressor::default);
                    if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                        // Only sanitized errors: never log credentials, URL or payloads.
                        tracing::error!(error = %error.as_report(), ?delay, suppressed_count, function = self.name, "AI request failed, retrying");
                    }
                }
            }
            // Sleep remains cancellable, including when a configured zero delay yields.
            tokio::time::sleep(delay).await;
            backoff_ms = backoff_ms
                .saturating_mul(2)
                .min(self.config.max_retry_delay_ms);
        }
    }
}
