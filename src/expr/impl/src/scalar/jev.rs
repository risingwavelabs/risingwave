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

use std::time::Duration;

use futures::{StreamExt, TryStreamExt, stream};
use reqwest::{Client, StatusCode};
use risingwave_common::array::{Array, ArrayBuilder, ArrayRef, DataChunk, JsonbArrayBuilder};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, DatumRef, JsonbRef, JsonbVal, ScalarImpl};
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, BoxedExpression, ExpressionInfo,
};
use risingwave_expr::{ExprError, Result, build_function};
use serde::Serialize;
use tokio::sync::Semaphore;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(5);
const MAX_CONCURRENCY: usize = 8;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// One state is one request. An array in `state` is context, not an HTTP batch.
#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    state: jsonbb::ValueRef<'a>,
    questions: jsonbb::ValueRef<'a>,
}

struct JevAnalyze {
    children: [BoxedExpression; 5],
    client: Client,
    // Shared by concurrent eval calls on this expression, including overlapping chunks.
    permits: Semaphore,
}

impl std::fmt::Debug for JevAnalyze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Arguments may contain credentials or customer state.
        f.debug_struct("JevAnalyze").finish_non_exhaustive()
    }
}

#[build_function(
    "jev_analyze(jsonb, varchar, varchar, jsonb, varchar) -> jsonb",
    volatile
)]
fn build(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let children = children.try_into().map_err(|_| ExprError::InvalidParam {
        name: "jev_analyze",
        reason: "expected five arguments".into(),
    })?;
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| request_error("could not initialize HTTP client"))?;
    Ok(JevAnalyze {
        children,
        client,
        permits: Semaphore::new(MAX_CONCURRENCY),
    }
    .boxed())
}

fn request_error(message: impl std::fmt::Display) -> ExprError {
    // Do not attach the request, response body, URL, or function arguments to errors.
    ExprError::External(format!("jev_analyze: {message}"))
}

impl JevAnalyze {
    async fn request(
        &self,
        state: JsonbRef<'_>,
        url: &str,
        model: &str,
        questions: JsonbRef<'_>,
        api_key: &str,
    ) -> Result<JsonbVal> {
        let url = reqwest::Url::parse(url).map_err(|_| request_error("invalid model_url"))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(request_error(
                "model_url must be an HTTP(S) URL without embedded credentials",
            ));
        }
        let request = self
            .client
            .post(url)
            .bearer_auth(api_key)
            .json(&Request {
                model,
                state: state.into(),
                questions: questions.into(),
            })
            .build()
            .map_err(|_| request_error("could not build HTTP request"))?;
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("semaphore is not closed");

        for attempt in 0..MAX_ATTEMPTS {
            let mut delay = RETRY_DELAY * (1 << attempt);
            let result = match self
                .client
                .execute(request.try_clone().expect("JSON body is cloneable"))
                .await
            {
                Ok(mut response) => {
                    let status = response.status();
                    if status.is_success() {
                        if response
                            .content_length()
                            .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
                        {
                            return Err(request_error("response exceeds 16 MiB"));
                        }
                        let mut bytes = Vec::new();
                        let mut read_error = None;
                        loop {
                            match response.chunk().await {
                                Ok(Some(chunk)) => {
                                    if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                                        return Err(request_error("response exceeds 16 MiB"));
                                    }
                                    bytes.extend_from_slice(&chunk);
                                }
                                Ok(None) => break,
                                Err(err) => {
                                    read_error = Some(err);
                                    break;
                                }
                            }
                        }
                        if let Some(err) = read_error {
                            Err(err)
                        } else {
                            // Preserve the entire response, including unknown metadata fields.
                            return jsonbb::Value::from_text(&bytes)
                                .map(JsonbVal::from)
                                .map_err(|_| {
                                    request_error("successful response is not valid JSON")
                                });
                        }
                    } else {
                        if !(status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
                            || attempt + 1 == MAX_ATTEMPTS
                        {
                            return Err(request_error(format!("HTTP {status}")));
                        }
                        if let Some(seconds) = response
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.parse::<u64>().ok())
                        {
                            delay = Duration::from_secs(seconds).min(MAX_RETRY_AFTER);
                        }
                        // The response is dropped before sleeping; do not retain its body.
                        Ok(())
                    }
                }
                Err(err) => Err(err),
            };
            if let Err(err) = result {
                let transient =
                    err.is_timeout() || err.is_connect() || err.is_body() || err.is_request();
                if !transient || attempt + 1 == MAX_ATTEMPTS {
                    return Err(request_error(if err.is_timeout() {
                        "HTTP request timed out"
                    } else {
                        "HTTP transport failed"
                    }));
                }
            }
            tokio::time::sleep(delay).await;
        }
        unreachable!("last attempt always returns")
    }

    async fn evaluate(&self, args: [DatumRef<'_>; 5]) -> Result<Option<JsonbVal>> {
        let [
            Some(state),
            Some(url),
            Some(model),
            Some(questions),
            Some(api_key),
        ] = args
        else {
            return Ok(None);
        };
        self.request(
            state.into_jsonb(),
            url.into_utf8(),
            model.into_utf8(),
            questions.into_jsonb(),
            api_key.into_utf8(),
        )
        .await
        .map(Some)
    }
}

impl ExpressionInfo for JevAnalyze {
    fn return_type(&self) -> DataType {
        DataType::Jsonb
    }
}

impl AsyncExpression for JevAnalyze {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut arrays = Vec::with_capacity(5);
        for child in &self.children {
            arrays.push(child.eval(input).await?);
        }
        let mut results = stream::iter(0..input.capacity())
            .map(|index| {
                let arrays = &arrays;
                async move {
                    if !input.visibility().is_set(index) {
                        return Ok(None);
                    }
                    self.evaluate(std::array::from_fn(|i| arrays[i].value_at(index)))
                        .await
                }
            })
            .buffered(MAX_CONCURRENCY);
        let mut builder = JsonbArrayBuilder::new(input.capacity());
        while let Some(result) = results.try_next().await? {
            builder.append_owned(result);
        }
        Ok(builder.finish().into_ref())
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut args = Vec::with_capacity(5);
        for child in &self.children {
            args.push(child.eval_row(input).await?);
        }
        self.evaluate(std::array::from_fn(|i| {
            args[i].as_ref().map(ScalarImpl::as_scalar_ref_impl)
        }))
        .await
        .map(|value| value.map(ScalarImpl::Jsonb))
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use risingwave_common::array::Utf8Array;
    use risingwave_common::types::Scalar;
    use risingwave_expr::expr::{InputRefExpression, SyncExpressionBoxExt};
    use serde_json::{Value, json};

    use super::*;

    struct Server {
        url: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn server(router: Router) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Server { url, task }
    }

    fn expression(timeout: Duration) -> JevAnalyze {
        JevAnalyze {
            children: std::array::from_fn(|i| {
                let ty = if i == 0 || i == 3 {
                    DataType::Jsonb
                } else {
                    DataType::Varchar
                };
                InputRefExpression::new(ty, i).boxed()
            }),
            client: Client::builder()
                .timeout(timeout)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            permits: Semaphore::new(MAX_CONCURRENCY),
        }
    }

    async fn call(expr: &JevAnalyze, url: &str) -> Result<JsonbVal> {
        expr.request(
            JsonbVal::from(json!({"messages": ["收到退款了"]})).as_scalar_ref(),
            url,
            "test-model",
            JsonbVal::from(json!({"resolved": {"type": "noul", "instructions": "已解决？"}}))
                .as_scalar_ref(),
            "test-only-key",
        )
        .await
    }

    #[tokio::test]
    async fn request_and_full_response() {
        let server = server(Router::new().route("/evaluate", post(|headers: HeaderMap, Json(body): Json<Value>| async move {
            assert_eq!(headers["authorization"], "Bearer test-only-key");
            assert_eq!(body["model"], "test-model");
            assert_eq!(body["state"]["messages"][0], "收到退款了");
            assert_eq!(body["questions"]["resolved"]["type"], "noul");
            Json(json!({"answers": {"resolved": {"type": "noul", "noul": 0.99}}, "unknown_metadata": [1, null, true]}))
        }))).await;
        let expr = expression(REQUEST_TIMEOUT);
        assert_eq!(
            call(&expr, &server.url).await.unwrap().take(),
            json!({
                "answers": {"resolved": {"type": "noul", "noul": 0.99}},
                "unknown_metadata": [1, null, true],
            })
        );
        assert!(!format!("{expr:?}").contains("test-only-key"));
    }

    #[tokio::test]
    async fn retries_transient_statuses_and_stops_after_limit() {
        for (statuses, expected, calls) in [
            (vec![429, 503, 200], true, 3),
            (vec![500, 500, 500, 200], false, 3),
            (vec![401, 200], false, 1),
            (vec![400, 200], false, 1),
        ] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let seen = attempts.clone();
            let statuses = Arc::new(statuses);
            let server = server(Router::new().route(
                "/evaluate",
                post(move || {
                    let index = seen.fetch_add(1, Ordering::SeqCst);
                    let status = statuses[index];
                    async move {
                        (
                            StatusCode::from_u16(status).unwrap(),
                            [("retry-after", "0")],
                            Json(json!({"ok": true})),
                        )
                    }
                }),
            ))
            .await;
            let result = call(&expression(REQUEST_TIMEOUT), &server.url).await;
            assert_eq!(result.is_ok(), expected);
            assert_eq!(attempts.load(Ordering::SeqCst), calls);
        }
    }

    #[tokio::test]
    async fn malformed_json_and_redirects_are_not_retried() {
        for status in [StatusCode::OK, StatusCode::TEMPORARY_REDIRECT] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let seen = attempts.clone();
            let server = server(Router::new().route(
                "/evaluate",
                post(move || {
                    seen.fetch_add(1, Ordering::SeqCst);
                    async move {
                        (
                            status,
                            [("location", "/evaluate")],
                            "test-only-key is not JSON",
                        )
                            .into_response()
                    }
                }),
            ))
            .await;
            let err = call(&expression(REQUEST_TIMEOUT), &server.url)
                .await
                .unwrap_err();
            assert!(!format!("{err:?}").contains("test-only-key"));
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn retries_timeouts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let seen = attempts.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move || {
                seen.fetch_add(1, Ordering::SeqCst);
                async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Json(json!({}))
                }
            }),
        ))
        .await;
        let err = call(&expression(Duration::from_millis(100)), &server.url)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out"));
        assert_eq!(attempts.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    fn chunk(url: &str, count: usize) -> DataChunk {
        let mut states = JsonbArrayBuilder::new(count);
        let mut questions = JsonbArrayBuilder::new(count);
        for i in 0..count {
            states.append_owned((i != 1).then(|| JsonbVal::from(json!({"index": i}))));
            questions.append_owned(Some(JsonbVal::from(json!({"q": {"type": "noul"}}))));
        }
        DataChunk::new(
            vec![
                states.finish().into_ref(),
                Utf8Array::from_iter(std::iter::repeat_n(Some(url), count)).into_ref(),
                Utf8Array::from_iter(std::iter::repeat_n(Some("test-model"), count)).into_ref(),
                questions.finish().into_ref(),
                Utf8Array::from_iter(std::iter::repeat_n(Some("test-only-key"), count)).into_ref(),
            ],
            (0..count)
                .map(|i| i != 2)
                .collect::<risingwave_common::bitmap::Bitmap>(),
        )
    }

    #[tokio::test]
    async fn preserves_order_skips_null_and_invisible_rows_and_bounds_concurrency() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let (a, m, r) = (active.clone(), maximum.clone(), requests.clone());
        let server = server(Router::new().route(
            "/evaluate",
            post(move |Json(body): Json<Value>| {
                let (a, m, r) = (a.clone(), m.clone(), r.clone());
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    let inflight = a.fetch_add(1, Ordering::SeqCst) + 1;
                    m.fetch_max(inflight, Ordering::SeqCst);
                    let index = body["state"]["index"].as_u64().unwrap();
                    tokio::time::sleep(Duration::from_millis(if index == 0 { 80 } else { 5 }))
                        .await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    Json(json!({"index": index}))
                }
            }),
        ))
        .await;
        let expr = expression(REQUEST_TIMEOUT);
        let input = chunk(&server.url, 20);
        // The semaphore bounds overlapping eval calls as well as a single chunk.
        let (left, right) = tokio::join!(expr.eval(&input), expr.eval(&input));
        for output in [left.unwrap(), right.unwrap()] {
            for i in 0..20 {
                let value = output
                    .as_jsonb()
                    .value_at(i)
                    .map(|v| JsonbVal::from(v).take());
                assert_eq!(value, (![1, 2].contains(&i)).then(|| json!({"index": i})));
            }
        }
        assert_eq!(requests.load(Ordering::SeqCst), 36);
        assert!((2..=MAX_CONCURRENCY).contains(&maximum.load(Ordering::SeqCst)));
    }

    #[tokio::test]
    async fn any_null_argument_skips_request() {
        let expr = expression(REQUEST_TIMEOUT);
        for null_index in 0..5 {
            let mut args = vec![
                Some(ScalarImpl::Jsonb(JsonbVal::empty_object())),
                Some(ScalarImpl::Utf8("invalid URL".into())),
                Some(ScalarImpl::Utf8("model".into())),
                Some(ScalarImpl::Jsonb(JsonbVal::empty_object())),
                Some(ScalarImpl::Utf8("key".into())),
            ];
            args[null_index] = None;
            assert!(expr.eval_row(&OwnedRow::new(args)).await.unwrap().is_none());
        }
    }
}
