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
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Datum, DatumRef, F64, JsonbRef, JsonbVal, ListValue, MapValue, ScalarImpl,
    StructValue,
};
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, BoxedExpression, ExpressionInfo,
};
use risingwave_expr::scalar::ai_decide::{QuestionKind, QuestionSchema};
use risingwave_expr::{ExprError, Result, build_function};
use serde::Serialize;
use tokio::sync::Semaphore;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(5);
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONCURRENCY: usize = 8;

/// One state is one request. An array in `state` is context, not an HTTP batch.
#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    state: jsonbb::ValueRef<'a>,
    questions: jsonbb::ValueRef<'a>,
}

struct AiDecide {
    children: [BoxedExpression; 5],
    schema: QuestionSchema,
    client: Client,
    // Shared by concurrent eval calls on this expression, including overlapping chunks.
    permits: Semaphore,
    max_concurrency: usize,
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
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| request_error("could not initialize HTTP client"))?;
    Ok(AiDecide {
        children,
        schema,
        client,
        permits: Semaphore::new(MAX_CONCURRENCY),
        max_concurrency: MAX_CONCURRENCY,
    }
    .boxed())
}

fn request_error(message: impl std::fmt::Display) -> ExprError {
    // Do not attach the request, response body, URL, or function arguments to errors.
    ExprError::External(format!("ai_decide: {message}"))
}

/// Decode strictly before the result can be persisted. Contract errors must survive
/// non-strict streaming wrappers, just like transport errors.
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
                            // Decode the full JSON envelope before validating typed answers.
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

    async fn evaluate(&self, args: [DatumRef<'_>; 5]) -> Result<Option<StructValue>> {
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
        .and_then(|response| decode_response(response, &self.schema))
        .map(Some)
    }
}

impl ExpressionInfo for AiDecide {
    fn return_type(&self) -> DataType {
        self.schema.return_type()
    }
}

impl AsyncExpression for AiDecide {
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
                        return Ok((index, None));
                    }
                    let value = self
                        .evaluate(std::array::from_fn(|i| arrays[i].value_at(index)))
                        .await?;
                    Ok::<_, ExprError>((index, value))
                }
            })
            .buffer_unordered(self.max_concurrency);
        // Refill HTTP slots as soon as requests finish, independently of row order.
        // The expression still returns one ordered array only after the whole chunk finishes.
        let mut ordered_results = vec![None; input.capacity()];
        while let Some((index, result)) = results.try_next().await? {
            ordered_results[index] = result;
        }
        let mut builder = self.return_type().create_array_builder(input.capacity());
        for result in ordered_results {
            builder.append(result.map(ScalarImpl::Struct));
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
        .map(|value| value.map(ScalarImpl::Struct))
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
    use risingwave_common::array::{Array, ArrayBuilder, JsonbArrayBuilder, Utf8Array};
    use risingwave_common::types::Scalar;
    use risingwave_expr::expr::{
        InputRefExpression, LiteralExpression, SyncExpressionBoxExt, build_func,
    };
    use risingwave_pb::expr::expr_node::Type as ExprType;
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

    fn expression(timeout: Duration) -> AiDecide {
        expression_with_concurrency(timeout, MAX_CONCURRENCY)
    }

    fn expression_with_concurrency(timeout: Duration, max_concurrency: usize) -> AiDecide {
        AiDecide {
            schema: test_schema(),
            children: std::array::from_fn(|i| {
                let ty = if i == 0 || i == 3 {
                    DataType::Jsonb
                } else {
                    DataType::Varchar
                };
                if i == 3 {
                    LiteralExpression::new(DataType::Jsonb, Some(test_questions().into())).boxed()
                } else {
                    InputRefExpression::new(ty, i).boxed()
                }
            }),
            client: Client::builder()
                .timeout(timeout)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            permits: Semaphore::new(max_concurrency),
            max_concurrency,
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
            assert!(matches!(error, ExprError::External(_)), "{pointer}");
            assert!(!error.to_string().contains("test-only-key"));
        }
    }

    async fn call(expr: &AiDecide, url: &str) -> Result<JsonbVal> {
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
            let expr = expression(REQUEST_TIMEOUT);
            let result = call(&expr, &server.url).await;
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
                    Json(json!({"answers": {"q": {"type": "noul", "noul": index as f64 / 100.0}}}))
                }
            }),
        ))
        .await;
        let expr = expression_with_concurrency(REQUEST_TIMEOUT, 3);
        let input = chunk(&server.url, 20);
        // The semaphore bounds overlapping eval calls as well as a single chunk.
        let (left, right) = tokio::join!(expr.eval(&input), expr.eval(&input));
        for output in [left.unwrap(), right.unwrap()] {
            for i in 0..20 {
                assert_eq!(
                    score_at(&output, i),
                    (![1, 2].contains(&i)).then_some(i as f64 / 100.0)
                );
            }
        }
        assert_eq!(requests.load(Ordering::SeqCst), 36);
        assert_eq!(maximum.load(Ordering::SeqCst), 3);
        assert_eq!(expr.permits.available_permits(), 3);
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
    async fn refills_slots_behind_a_blocked_first_row_and_restores_order() {
        for max_concurrency in [2, 8, 16] {
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
            let expr = expression_with_concurrency(REQUEST_TIMEOUT, max_concurrency);
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
                    }).await.expect("completed rows must release scheduling slots");
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

    #[tokio::test]
    async fn failure_cancels_pending_rows_and_releases_permits() {
        let server = server(Router::new().route(
            "/evaluate",
            post(|Json(body): Json<Value>| async move {
                match body["state"]["index"].as_u64().unwrap() {
                    0 => futures::future::pending().await,
                    3 => (
                        StatusCode::OK,
                        Json(json!({"answers":{"q":{"type":"noul","noul":0.03}}})),
                    ),
                    4 => (StatusCode::UNAUTHORIZED, Json(json!({}))),
                    _ => unreachable!(),
                }
            }),
        ))
        .await;
        let expr = expression_with_concurrency(REQUEST_TIMEOUT, 2);
        let input = chunk(&server.url, 5);
        let error = tokio::time::timeout(Duration::from_secs(5), expr.eval(&input))
            .await
            .expect("later failures must not wait for the blocked first row")
            .unwrap_err();
        assert!(error.to_string().contains("401"));
        assert_eq!(expr.permits.available_permits(), 2);
    }

    #[tokio::test]
    async fn cancellation_cleans_up_waiters_across_overlapping_evaluations() {
        let started = Arc::new(Semaphore::new(0));
        let seen = started.clone();
        let server = server(Router::new().route(
            "/evaluate",
            post(move || {
                seen.add_permits(1);
                async { futures::future::pending::<Json<Value>>().await }
            }),
        ))
        .await;
        let expr = expression_with_concurrency(REQUEST_TIMEOUT, 1);
        {
            let requests =
                async { tokio::join!(call(&expr, &server.url), call(&expr, &server.url)) };
            futures::pin_mut!(requests);
            tokio::select! {
                _ = &mut requests => panic!("server never completes"),
                _ = async {
                    tokio::time::timeout(Duration::from_secs(5), started.acquire())
                        .await.unwrap().unwrap().forget();
                } => {}
            }
            // Drop both request futures, including the semaphore waiter.
        }
        assert_eq!(expr.permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn registered_expression_shares_the_fixed_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (a, m) = (active.clone(), maximum.clone());
        let server = server(Router::new().route(
            "/evaluate",
            post(move |Json(body): Json<Value>| {
                let (a, m, started, gate) =
                    (a.clone(), m.clone(), started.clone(), gate.clone());
                async move {
                    let inflight = a.fetch_add(1, Ordering::SeqCst) + 1;
                    m.fetch_max(inflight, Ordering::SeqCst);
                    if started.fetch_add(1, Ordering::SeqCst) + 1 == MAX_CONCURRENCY {
                        gate.add_permits(36);
                    }
                    gate.acquire().await.unwrap().forget();
                    tokio::task::yield_now().await;
                    a.fetch_sub(1, Ordering::SeqCst);
                    Json(json!({"answers":{"q":{"type":"noul","noul":body["state"]["index"].as_f64().unwrap()/100.0}}}))
                }
            }),
        ))
        .await;
        let children = expression(REQUEST_TIMEOUT).children.into_iter().collect();
        let expr = build_func(ExprType::AiDecide, test_schema().return_type(), children).unwrap();
        let input = chunk(&server.url, 20);
        let (left, right) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(expr.eval(&input), expr.eval(&input))
        })
        .await
        .expect("the fixed limit must allow the initial request group to start");
        assert!(left.is_ok() && right.is_ok());
        assert_eq!(maximum.load(Ordering::SeqCst), MAX_CONCURRENCY);
    }
}
