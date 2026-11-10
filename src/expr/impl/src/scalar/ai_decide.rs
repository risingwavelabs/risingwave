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

use std::sync::LazyLock;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use reqwest::Client;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::config::AiDecideConfig;
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Datum, DatumRef, F64, JsonbRef, JsonbVal, ListValue, MapValue, Scalar, ScalarImpl,
    StructValue, ToDatumRef,
};
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, BoxedExpression, ExpressionInfo, ValueImpl,
};
use risingwave_expr::scalar::ai_decide::{QuestionKind, QuestionSchema};
use risingwave_expr::{ExprError, Result, build_function};
use serde::Serialize;
use tokio::sync::Semaphore;

/// One state is one request. An array in `state` is context, not an HTTP batch.
#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    state: jsonbb::ValueRef<'a>,
    questions: jsonbb::ValueRef<'a>,
}

struct AiDecide {
    children: [BoxedExpression; 4],
    questions: JsonbVal,
    return_type: DataType,
    schema: QuestionSchema,
    client: Client,
    config: AiDecideConfig,
    request_permits: Option<Semaphore>,
}

impl std::fmt::Debug for AiDecide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Arguments may contain credentials or customer state.
        f.debug_struct("AiDecide").finish_non_exhaustive()
    }
}

#[build_function(
    "ai_decide(jsonb, varchar, varchar, jsonb, varchar) -> struct",
    volatile,
    async,
    type_infer = "unreachable"
)]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let children: [BoxedExpression; 5] =
        children.try_into().map_err(|_| ExprError::InvalidParam {
            name: "ai_decide",
            reason: "expected exactly five arguments".into(),
        })?;
    let Some(ScalarImpl::Jsonb(questions)) = children[3].eval_const()? else {
        return Err(ExprError::InvalidParam {
            name: "ai_decide questions",
            reason: "must be constant non-null JSONB".into(),
        });
    };
    let schema = QuestionSchema::parse(&questions)?;
    if return_type != schema.return_type() {
        return Err(ExprError::InvalidParam {
            name: "ai_decide",
            reason: "return type does not match fixed questions".into(),
        });
    }
    let config = risingwave_expr::expr_context::ai_decide_config();
    config
        .validate()
        .map_err(|reason| ExprError::InvalidParam {
            name: "ai_decide config",
            reason: reason.into(),
        })?;
    if config.max_concurrency as usize > Semaphore::MAX_PERMITS {
        return Err(ExprError::InvalidParam {
            name: "ai_decide_max_concurrency",
            reason: "exceeds semaphore capacity".into(),
        });
    }
    let request_permits =
        (config.max_concurrency != 0).then(|| Semaphore::new(config.max_concurrency as usize));
    let client = Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms.into()))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| request_error("could not initialize HTTP client"))?;
    let [state, url, model, _, api_key] = children;
    Ok(AiDecide {
        children: [state, url, model, api_key],
        questions,
        return_type,
        schema,
        client,
        config,
        request_permits,
    }
    .boxed())
}

fn request_error(message: impl std::fmt::Display) -> ExprError {
    // Do not attach the request, response body, URL, or function arguments to errors.
    ExprError::Custom(format!("ai_decide: {message}"))
}

/// Decode strictly before the result can be persisted. Invalid responses are retried
/// inside the request loop, without reaching non-strict expression wrappers.
fn decode_response(response: JsonbVal, schema: &QuestionSchema) -> Result<StructValue> {
    let response = response.take();
    let answers = response
        .get("answers")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| request_error("response must contain an answers object"))?;
    let mut fields = Vec::with_capacity(schema.0.len());
    for question in &schema.0 {
        let error = |field: &str| {
            request_error(format!(
                "invalid response for question {:?}: {field}",
                question.id
            ))
        };
        let answer = answers
            .get(&question.id)
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| error("missing answer object"))?;
        let expected = match &question.kind {
            QuestionKind::Noul => "noul",
            QuestionKind::Choice(_) => "choice",
            QuestionKind::Score(_) => "score",
        };
        if answer.get("type").and_then(serde_json::Value::as_str) != Some(expected) {
            return Err(error("answer type does not match the question"));
        }
        let number = |field: &str, maximum: f64| -> Result<F64> {
            answer
                .get(field)
                .and_then(serde_json::Value::as_f64)
                .filter(|v| v.is_finite() && (0.0..=maximum).contains(v))
                .map(F64::from)
                .ok_or_else(|| error(field))
        };
        let value = if let QuestionKind::Noul = &question.kind {
            ScalarImpl::Float64(number("noul", 1.0)?)
        } else {
            let confidence = number("confidence", 1.0)?;
            let probabilities = answer
                .get("probabilities")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| error("missing probabilities object"))?;
            let keys = match &question.kind {
                QuestionKind::Choice(labels) => labels.clone(),
                QuestionKind::Score(count) => (0..*count).map(|i| i.to_string()).collect(),
                QuestionKind::Noul => unreachable!(),
            };
            if probabilities.len() != keys.len() {
                return Err(error(
                    "probabilities must contain exactly the declared options",
                ));
            }
            let distribution = keys
                .iter()
                .map(|key| {
                    probabilities
                        .get(key)
                        .and_then(serde_json::Value::as_f64)
                        .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
                        .ok_or_else(|| error("missing or invalid probability"))
                })
                .collect::<Result<Vec<_>>>()?;
            // Services may round each probability independently. Preserve their values
            // rather than imposing a precision or silently renormalizing the answer.
            let distribution = ListValue::from_iter(distribution.into_iter().map(F64::from));
            let (value, probabilities) = match &question.kind {
                QuestionKind::Choice(labels) => {
                    let choice = answer
                        .get("choice")
                        .and_then(serde_json::Value::as_str)
                        .filter(|s| labels.iter().any(|label| label == s))
                        .ok_or_else(|| error("choice must be a declared option"))?;
                    let probabilities = MapValue::try_from_kv(
                        ListValue::from_iter(labels.iter().map(String::as_str)),
                        distribution,
                    )
                    .map_err(|_| error("invalid probability map"))?;
                    (
                        ScalarImpl::Utf8(choice.into()),
                        ScalarImpl::Map(probabilities),
                    )
                }
                QuestionKind::Score(count) => (
                    ScalarImpl::Float64(number("score", (count - 1) as f64)?),
                    ScalarImpl::List(distribution),
                ),
                QuestionKind::Noul => unreachable!(),
            };
            ScalarImpl::Struct(StructValue::new(vec![
                Some(value),
                Some(confidence.into()),
                Some(probabilities),
            ]))
        };
        fields.push(Some(value));
    }
    Ok(StructValue::new(fields))
}

impl AiDecide {
    async fn request(
        &self,
        state: JsonbRef<'_>,
        url: &str,
        model: &str,
        api_key: &str,
    ) -> Result<StructValue> {
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
                questions: self.questions.as_scalar_ref().into(),
            })
            .build()
            .map_err(|_| request_error("could not build HTTP request"))?;
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
                    .execute(request.try_clone().expect("JSON body is cloneable"))
                    .await
                    .map_err(|err| {
                        request_error(if err.is_timeout() {
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
                    return Err(request_error(format!("HTTP {status}")));
                }
                let too_large = || {
                    request_error(format!(
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
                    .map_err(|_| request_error("HTTP response body read failed"))?
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
                    .map_err(|_| request_error("successful response is not valid JSON"))?;
                decode_response(json, &self.schema)
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                        LazyLock::new(LogSuppressor::default);
                    if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                        // Only sanitized errors: never log credentials, URL or payloads.
                        tracing::error!(error = %error, ?delay, suppressed_count, "AI_DECIDE request failed, retrying");
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

    async fn evaluate(&self, args: [DatumRef<'_>; 4]) -> Result<Option<StructValue>> {
        let [Some(state), Some(url), Some(model), Some(api_key)] = args else {
            return Ok(None);
        };
        // Share permits across concurrent eval/eval_row calls on this expression instance.
        // Retain the permit through retries and backoff; cancellation releases it by drop.
        let _permit = match &self.request_permits {
            Some(permits) => Some(
                permits
                    .acquire()
                    .await
                    .expect("request semaphore is never closed"),
            ),
            None => None,
        };
        self.request(
            state.into_jsonb(),
            url.into_utf8(),
            model.into_utf8(),
            api_key.into_utf8(),
        )
        .await
        .map(Some)
    }
}

impl ExpressionInfo for AiDecide {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }
}

impl AsyncExpression for AiDecide {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut values = Vec::with_capacity(4);
        for child in &self.children {
            values.push(child.eval_v2(input).await?);
        }
        let mut results = (0..input.capacity())
            .map(|index| {
                let values = &values;
                async move {
                    if !input.visibility().is_set(index) {
                        return (index, Ok(None));
                    }
                    let value = self
                        .evaluate(std::array::from_fn(|i| match &values[i] {
                            ValueImpl::Array(array) => array.value_at(index),
                            ValueImpl::Scalar { value, .. } => value.to_datum_ref(),
                        }))
                        .await;
                    (index, value)
                }
            })
            .collect::<FuturesUnordered<_>>();
        // Preserve successful rows when another row cannot construct its request.
        // NonStrict uses the partial array in Multiple; strict wrappers still report an error.
        let mut ordered_results = vec![None; input.capacity()];
        let mut errors = Vec::new();
        while let Some((index, result)) = results.next().await {
            match result {
                Ok(value) => ordered_results[index] = value,
                Err(error) => errors.push(error),
            }
        }
        let mut builder = self.return_type().create_array_builder(input.capacity());
        for result in ordered_results {
            builder.append(result.map(ScalarImpl::Struct));
        }
        let array = builder.finish().into_ref();
        if errors.is_empty() {
            Ok(array)
        } else {
            Err(ExprError::Multiple(array, errors.into()))
        }
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut args = Vec::with_capacity(4);
        for child in &self.children {
            args.push(child.eval_row(input).await?);
        }
        self.evaluate(std::array::from_fn(|i| {
            args[i].as_ref().map(ScalarImpl::as_scalar_ref_impl)
        }))
        .await
        .map(|value| value.map(ScalarImpl::Struct))
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use risingwave_common::array::{Array, ArrayBuilder, JsonbArrayBuilder, Utf8Array};
    use risingwave_expr::expr::{
        InputRefExpression, LiteralExpression, LogReport, NonStrictExpression,
        SyncExpressionBoxExt, build_func,
    };
    use risingwave_pb::expr::expr_node::Type as ExprType;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Barrier;

    use super::*;

    const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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

    fn expression(timeout: Duration) -> AiDecide {
        AiDecide {
            schema: test_schema(),
            questions: test_questions(),
            return_type: test_schema().return_type(),
            children: [0, 1, 2, 4].map(|i| {
                let ty = if i == 0 {
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
            config: AiDecideConfig::default(),
            request_permits: None,
        }
    }

    fn test_questions() -> JsonbVal {
        JsonbVal::from(json!({"q": {"type": "noul", "instructions": "Test probability?"}}))
    }

    fn test_schema() -> QuestionSchema {
        QuestionSchema::parse(&test_questions()).unwrap()
    }

    fn score_at(array: &ArrayRef, index: usize) -> Option<f64> {
        array
            .as_struct()
            .value_at(index)
            .map(|value| value.field_at(0).unwrap().into_float64().0)
    }

    #[test]
    fn decodes_mixed_answers_without_losing_distributions() {
        let schema = QuestionSchema::parse(&JsonbVal::from(json!({
            "refund":{"type":"noul","instructions":"Refund?"},
            "team":{"type":"choice","instructions":"Team?","criteria":{"billing":"Payments","tech":"Bugs"}},
            "urgency":{"type":"score","instructions":"Urgent?","criteria":["Routine","Soon","Blocking"]}
        }))).unwrap();
        let response = json!({"answers":{
            "refund":{"type":"noul","noul":0.8},
            "team":{"type":"choice","choice":"billing","confidence":0.6,"probabilities":{"tech":0.2,"billing":0.8}},
            "urgency":{"type":"score","score":1.25,"confidence":0.4,"probabilities":{"2":0.5,"0":0.25,"1":0.25}}
        },"unknown_metadata":true});
        let result = decode_response(JsonbVal::from(response.clone()), &schema).unwrap();
        assert_eq!(result.fields()[0], Some(ScalarImpl::Float64(0.8.into())));
        let team = result.fields()[1].as_ref().unwrap().as_struct();
        assert_eq!(team.fields()[0], Some(ScalarImpl::Utf8("billing".into())));
        assert_eq!(team.fields()[1], Some(ScalarImpl::Float64(0.6.into())));
        let expected_map = MapValue::try_from_kv(
            ListValue::from_iter(["billing", "tech"]),
            ListValue::from_iter([F64::from(0.8), F64::from(0.2)]),
        )
        .unwrap();
        assert_eq!(team.fields()[2], Some(ScalarImpl::Map(expected_map)));
        let urgency = result.fields()[2].as_ref().unwrap().as_struct();
        assert_eq!(urgency.fields()[0], Some(ScalarImpl::Float64(1.25.into())));
        assert_eq!(
            urgency.fields()[2],
            Some(ScalarImpl::List(ListValue::from_iter([
                F64::from(0.25),
                F64::from(0.25),
                F64::from(0.5)
            ])))
        );

        let mut rounded = response.clone();
        rounded["answers"]["urgency"]["probabilities"] =
            json!({"0": 0.3333, "1": 0.3333, "2": 0.3333});
        let rounded = decode_response(JsonbVal::from(rounded), &schema).unwrap();
        assert_eq!(
            rounded.fields()[2].as_ref().unwrap().as_struct().fields()[2],
            Some(ScalarImpl::List(ListValue::from_iter(
                [F64::from(0.3333); 3]
            )))
        );

        for (pointer, value) in [
            ("/answers/refund", Value::Null),
            ("/answers/refund/noul", json!(1.1)),
            ("/answers/refund/noul", json!("test-only-key")),
            ("/answers/team/type", json!("score")),
            ("/answers/team/choice", json!("undeclared")),
            ("/answers/team/confidence", Value::Null),
            ("/answers/team/probabilities", json!({"billing":0.8})),
            (
                "/answers/team/probabilities",
                json!({"billing":0.8,"other":0.2}),
            ),
            ("/answers/team/probabilities/tech", json!(1.2)),
            ("/answers/team/probabilities/tech", json!(-0.2)),
            ("/answers/urgency/score", json!(3.0)),
            ("/answers/urgency/probabilities", json!([0.25, 0.25, 0.5])),
        ] {
            let mut malformed = response.clone();
            *malformed.pointer_mut(pointer).unwrap() = value;
            let error = decode_response(JsonbVal::from(malformed), &schema).unwrap_err();
            assert!(matches!(error, ExprError::Custom(_)), "{pointer}");
            assert!(!error.to_string().contains("test-only-key"));
        }
    }

    async fn call(expr: &AiDecide, url: &str) -> Result<StructValue> {
        expr.request(
            JsonbVal::from(json!({"messages": ["收到退款了"]})).as_scalar_ref(),
            url,
            "test-model",
            "test-only-key",
        )
        .await
    }

    #[tokio::test]
    async fn scalar_arguments_and_request_contract() {
        let server = server(Router::new().route("/evaluate", post(|headers: HeaderMap, Json(body): Json<Value>| async move {
            assert_eq!(headers["authorization"], "Bearer test-only-key");
            assert_eq!(body["model"], "test-model");
            assert_eq!(body["state"]["messages"][0], "收到退款了");
            assert_eq!(body["questions"]["q"]["instructions"], "已解决？");
            Json(json!({"answers": {"q": {"type": "noul", "noul": 0.99}}, "unknown_metadata": [1, null, true]}))
        }))).await;
        let mut expr = expression(REQUEST_TIMEOUT);
        expr.questions = JsonbVal::from(json!({"q": {"type": "noul", "instructions": "已解决？"}}));
        expr.children = [
            LiteralExpression::new(
                DataType::Jsonb,
                Some(JsonbVal::from(json!({"messages": ["收到退款了"]})).into()),
            )
            .boxed(),
            LiteralExpression::new(DataType::Varchar, Some(server.url.clone().into())).boxed(),
            LiteralExpression::new(DataType::Varchar, Some("test-model".into())).boxed(),
            LiteralExpression::new(DataType::Varchar, Some("test-only-key".into())).boxed(),
        ];
        // The credential is actually present in a child, so formatting the children
        // would make this assertion fail.
        assert!(format!("{:?}", expr.children[3]).contains("test-only-key"));
        assert!(!format!("{expr:?}").contains("test-only-key"));
        let output = expr.eval(&DataChunk::new_dummy(3)).await.unwrap();
        for i in 0..3 {
            assert_eq!(score_at(&output, i), Some(0.99));
        }
        assert_eq!(
            expr.eval_row(&OwnedRow::empty()).await.unwrap(),
            output.datum_at(0)
        );
    }

    // Raw HTTP permits deliberately truncated and chunked responses that an HTTP
    // framework would repair or reject before sending.
    async fn raw_server(responses: Vec<Vec<u8>>, attempts: Arc<AtomicUsize>) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/evaluate", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut first_body = None;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let header_end = loop {
                    let byte = socket.read_u8().await.unwrap();
                    request.push(byte);
                    if request.ends_with(b"\r\n\r\n") {
                        break request.len();
                    }
                };
                let headers = String::from_utf8_lossy(&request).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                request.resize(header_end + length, 0);
                socket.read_exact(&mut request[header_end..]).await.unwrap();
                let body = request[header_end..].to_vec();
                if let Some(first) = &first_body {
                    assert_eq!(&body, first, "retries must preserve the request body");
                } else {
                    first_body = Some(body);
                }
                let index = attempts.fetch_add(1, Ordering::SeqCst);
                let response = &responses[index.min(responses.len() - 1)];
                // A size-limit rejection may close the socket before all bytes are sent.
                let _ = socket.write_all(response).await;
                let _ = socket.shutdown().await;
            }
        });
        Server { url, task }
    }

    fn complete_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn retries_all_remote_errors_until_a_valid_answer() {
        let good = r#"{"answers":{"q":{"type":"noul","noul":0.5}}}"#;
        let mut responses: Vec<_> = [429, 503, 401, 400, 307].into_iter().map(|status| {
            format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nRetry-After: 100\r\nLocation: /wrong\r\nConnection: close\r\n\r\n").into_bytes()
        }).collect();
        responses.extend([
            complete_response("test-only-key is not JSON"),
            complete_response(r#"{"answers":{"q":{"type":"noul","noul":2}}}"#),
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"answers\":".to_vec(),
            complete_response(&" ".repeat(129)),
            format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n81\r\n{}\r\n0\r\n\r\n", " ".repeat(129)).into_bytes(),
            complete_response(good),
        ]);
        let expected = responses.len();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server = raw_server(responses, attempts.clone()).await;
        let mut expr = expression(REQUEST_TIMEOUT);
        expr.config.max_response_bytes = 128;
        // Both Retry-After and exponential backoff must honor their configured caps.
        expr.config.max_retry_after_ms = 1;
        expr.config.retry_delay_ms = 10_000;
        expr.config.max_retry_delay_ms = 2;
        let value = tokio::time::timeout(Duration::from_secs(5), call(&expr, &server.url))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.fields()[0], Some(ScalarImpl::Float64(0.5.into())));
        assert_eq!(attempts.load(Ordering::SeqCst), expected);
    }

    #[tokio::test]
    async fn retries_timeouts_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let seen = attempts.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move || {
                let attempt = seen.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 4 {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Json(json!({"answers":{"q":{"type":"noul","noul":0.5}}}))
                }
            }),
        ))
        .await;
        let mut expr = expression(Duration::from_millis(50));
        expr.config.retry_delay_ms = 1;
        let value = tokio::time::timeout(Duration::from_secs(5), call(&expr, &server.url))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.fields()[0], Some(ScalarImpl::Float64(0.5.into())));
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
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
    async fn request_construction_errors_preserve_other_rows() {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move |Json(body): Json<Value>| {
                seen.fetch_add(1, Ordering::SeqCst);
                async move {
                    let index = body["state"]["index"].as_u64().unwrap();
                    Json(json!({"answers":{"q":{"type":"noul","noul":index as f64 / 10.0}}}))
                }
            }),
        ))
        .await;
        let (mut columns, visibility) = chunk(&server.url, 8).into_parts();
        columns[1] = Utf8Array::from_iter([
            Some(server.url.as_str()),
            Some("invalid"), // NULL state: no request or error.
            Some("invalid"), // Invisible row: no request or error.
            Some("invalid"),
            Some(server.url.as_str()),
            Some("ftp://example.com"),
            Some(server.url.as_str()),
            Some(server.url.as_str()),
        ])
        .into_ref();
        columns[4] = Utf8Array::from_iter(
            (0..8).map(|i| Some(if i == 6 { "secret\nkey" } else { "test-key" })),
        )
        .into_ref();
        let input = DataChunk::new(columns, visibility);
        let mut expr = expression(REQUEST_TIMEOUT);
        // Exercise good rows both before and after failures while sharing one permit.
        expr.request_permits = Some(Semaphore::new(1));
        let Err(ExprError::Multiple(partial, errors)) = expr.eval(&input).await else {
            panic!("row errors must carry the successful results");
        };
        let errors: Vec<_> = errors.into_iter().collect();
        assert_eq!(errors.len(), 3);
        assert!(
            errors
                .iter()
                .all(|error| !error.to_string().contains("secret"))
        );
        let expr = NonStrictExpression::new_topmost(expr.boxed(), LogReport);
        let output = expr.eval_infallible(&input).await;
        for array in [&partial, &output] {
            for i in 0..8 {
                assert_eq!(
                    score_at(array, i),
                    [0, 4, 7].contains(&i).then_some(i as f64 / 10.0)
                );
            }
        }
        assert_eq!(requests.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn registered_expression_evaluates_all_visible_rows_across_chunks() {
        // Each chunk has 20 rows, minus one NULL and one invisible row. Hold every request
        // until all 36 have arrived: neither a per-chunk nor a shared limit may block them.
        let ready = Arc::new(Barrier::new(36));
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move |Json(body): Json<Value>| {
                let (ready, seen) = (ready.clone(), seen.clone());
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    ready.wait().await;
                    let index = body["state"]["index"].as_u64().unwrap();
                    Json(json!({"answers": {"q": {"type": "noul", "noul": index as f64 / 100.0}}}))
                }
            }),
        ))
        .await;
        let mut children: Vec<_> = expression(REQUEST_TIMEOUT).children.into_iter().collect();
        children.insert(
            3,
            LiteralExpression::new(DataType::Jsonb, Some(test_questions().into())).boxed(),
        );
        let expr = build_func(ExprType::AiDecide, test_schema().return_type(), children).unwrap();
        let input = chunk(&server.url, 20);
        let (left, right) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(expr.eval(&input), expr.eval(&input))
        })
        .await
        .expect("all eligible rows in both chunks must start without an internal limit");
        for output in [left.unwrap(), right.unwrap()] {
            for i in 0..20 {
                assert_eq!(
                    score_at(&output, i),
                    (![1, 2].contains(&i)).then_some(i as f64 / 100.0)
                );
            }
        }
        assert_eq!(requests.load(Ordering::SeqCst), 36);

        let empty = expr.eval(&chunk(&server.url, 0)).await.unwrap();
        assert_eq!(empty.len(), 0);
        assert_eq!(requests.load(Ordering::SeqCst), 36);
    }

    #[tokio::test]
    async fn configured_limit_is_shared_across_chunks_and_released_on_cancellation() {
        let gate = Arc::new(Semaphore::new(0));
        let (started, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let handler_gate = gate.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move || {
                let gate = handler_gate.clone();
                let started = started.clone();
                async move {
                    started.send(()).unwrap();
                    gate.acquire().await.unwrap().forget();
                    Json(json!({"answers": {"q": {"type": "noul", "noul": 0.5}}}))
                }
            }),
        ))
        .await;
        let config = AiDecideConfig {
            max_concurrency: 2,
            ..Default::default()
        };
        let expr = risingwave_expr::expr_context::AI_DECIDE_CONFIG::scope(config, async {
            let mut children: Vec<_> = expression(REQUEST_TIMEOUT).children.into_iter().collect();
            children.insert(
                3,
                LiteralExpression::new(DataType::Jsonb, Some(test_questions().into())).boxed(),
            );
            Arc::new(build_func(ExprType::AiDecide, test_schema().return_type(), children).unwrap())
        })
        .await;
        let input = chunk(&server.url, 6);
        let cancelled = {
            let expr = expr.clone();
            let input = input.clone();
            tokio::spawn(async move { tokio::join!(expr.eval(&input), expr.eval(&input)) })
        };
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(5), requests.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), requests.recv())
                .await
                .is_err(),
            "two concurrent chunks must share the same two permits"
        );
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        let resumed = tokio::spawn(async move { expr.eval(&input).await });
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(5), requests.recv())
                .await
                .unwrap()
                .unwrap();
        }
        // Cancellation releases local permits; the server may still be processing old requests.
        gate.add_permits(20);
        let output = tokio::time::timeout(Duration::from_secs(5), resumed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(score_at(&output, 0), Some(0.5));
        assert_eq!(score_at(&output, 1), None);
        assert_eq!(score_at(&output, 2), None);
        assert_eq!(score_at(&output, 5), Some(0.5));
    }

    #[tokio::test]
    async fn persistent_failure_keeps_permit_until_cancelled() {
        let healthy = Arc::new(AtomicBool::new(false));
        let service_healthy = healthy.clone();
        let (started, mut attempts) = tokio::sync::mpsc::unbounded_channel();
        let server = server(Router::new().route(
            "/evaluate",
            post(move || {
                let healthy = service_healthy.load(Ordering::SeqCst);
                started.send(()).unwrap();
                async move {
                    if healthy {
                        Json(json!({"answers":{"q":{"type":"noul","noul":0.5}}})).into_response()
                    } else {
                        StatusCode::UNAUTHORIZED.into_response()
                    }
                }
            }),
        ))
        .await;
        let mut expr = expression(REQUEST_TIMEOUT);
        expr.config.retry_delay_ms = 10;
        expr.request_permits = Some(Semaphore::new(1));
        let expr = Arc::new(expr);
        let input = chunk(&server.url, 1);
        let task = {
            let expr = expr.clone();
            let input = input.clone();
            tokio::spawn(async move { expr.eval(&input).await })
        };
        for _ in 0..6 {
            tokio::time::timeout(Duration::from_secs(5), attempts.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            !task.is_finished(),
            "permanent HTTP failures must stay inside the retry loop"
        );
        assert_eq!(
            expr.request_permits.as_ref().unwrap().available_permits(),
            0
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            expr.request_permits.as_ref().unwrap().available_permits(),
            1
        );
        healthy.store(true, Ordering::SeqCst);
        let result = tokio::time::timeout(Duration::from_secs(5), expr.eval(&input))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(score_at(&result, 0), Some(0.5));
    }

    #[tokio::test]
    async fn any_null_argument_skips_request() {
        let expr = expression(REQUEST_TIMEOUT);
        for null_index in [0, 1, 2, 4] {
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

    #[tokio::test]
    async fn completes_later_rows_while_first_is_blocked_and_restores_order() {
        let first = Arc::new(Semaphore::new(0));
        let gate = first.clone();
        let completed = Arc::new(AtomicUsize::new(0));
        let seen = completed.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move |Json(body): Json<Value>| {
                let gate = gate.clone();
                let seen = seen.clone();
                async move {
                    let index = body["state"]["index"].as_u64().unwrap();
                    if index == 0 {
                        gate.acquire().await.unwrap().forget();
                    }
                    seen.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"answers": {"q": {"type": "noul", "noul": index as f64 / 100.0}}}))
                }
            }),
        ))
        .await;
        let expr = expression(REQUEST_TIMEOUT);
        let input = chunk(&server.url, 30);
        let evaluation = expr.eval(&input);
        futures::pin_mut!(evaluation);
        tokio::select! {
            result = &mut evaluation => panic!("first row was not released: {result:?}"),
            _ = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    // 30 rows minus SQL NULL, invisible row and the blocked first row.
                    while completed.load(Ordering::SeqCst) != 27 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }).await.expect("later rows must complete independently of the first row");
            } => {}
        }
        first.add_permits(1);
        let output = evaluation.await.unwrap();
        for i in 0..30 {
            assert_eq!(
                score_at(&output, i),
                (![1, 2].contains(&i)).then_some(i as f64 / 100.0)
            );
        }
    }
}
