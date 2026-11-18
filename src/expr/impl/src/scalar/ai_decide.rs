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

//! `AI_DECIDE` request encoding and typed response validation.

use risingwave_common::types::{
    DataType, F64, JsonbVal, ListValue, MapValue, Scalar, ScalarImpl, ScalarRefImpl, StructValue,
};
use risingwave_expr::expr::BoxedExpression;
use risingwave_expr::scalar::ai_decide::{QuestionKind, QuestionSchema};
use risingwave_expr::{ExprError, Result, build_function};
use serde::Serialize;

use super::ai_inference::{AiInference, InferenceCodec, function_error};

/// One state is one request. An array in `state` is context, not an HTTP batch.
#[derive(Serialize)]
struct Request<'a> {
    model: &'a str,
    state: jsonbb::ValueRef<'a>,
    questions: jsonbb::ValueRef<'a>,
}

pub(super) struct DecideCodec {
    definition: JsonbVal,
    schema: QuestionSchema,
}

impl InferenceCodec for DecideCodec {
    const DEFINITION: &'static str = "questions";
    const DEFINITION_NAME: &'static str = "ai_decide questions";
    const NAME: &'static str = "ai_decide";

    fn new(definition: JsonbVal) -> Result<Self> {
        let schema = QuestionSchema::parse(&definition)?;
        Ok(Self { definition, schema })
    }

    fn return_type(&self) -> DataType {
        self.schema.return_type()
    }

    fn encode(&self, content: ScalarRefImpl<'_>, model: &str) -> Result<Vec<u8>> {
        serde_json::to_vec(&Request {
            model,
            state: content.into_jsonb().into(),
            questions: self.definition.as_scalar_ref().into(),
        })
        .map_err(|_| function_error(Self::NAME, "could not build HTTP request"))
    }

    fn decode(&self, response: JsonbVal, _content: ScalarRefImpl<'_>) -> Result<ScalarImpl> {
        decode_response(response, &self.schema).map(ScalarImpl::Struct)
    }
}

#[build_function(
    "ai_decide(jsonb, varchar, varchar, jsonb, varchar) -> struct",
    volatile,
    async,
    type_infer = "unreachable"
)]
#[build_function(
    "ai_decide(jsonb, varchar, varchar, jsonb, varchar, anymap) -> struct",
    volatile,
    async,
    type_infer = "unreachable"
)]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    AiInference::<DecideCodec>::build(
        return_type,
        children,
        risingwave_expr::scalar::ai_decide::config(),
    )
}

fn request_error(message: impl std::fmt::Display) -> ExprError {
    function_error("ai_decide", message)
}

/// Decode strictly before the result can be persisted. Invalid responses are retried
/// inside the request loop, without reaching non-strict expression wrappers.
pub(super) fn decode_response(response: JsonbVal, schema: &QuestionSchema) -> Result<StructValue> {
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
