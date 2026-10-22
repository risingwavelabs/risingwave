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

use risingwave_common::types::{DataType, Datum, JsonbVal, ScalarImpl, ScalarRefImpl};
use risingwave_expr::scalar::ai_decide::QuestionSchema;
use serde_json::{Map, Value, json};

use crate::error::{ErrorCode, Result};
use crate::expr::{ExprImpl, ExprType, FunctionCall, Literal};

fn invalid(reason: impl Into<String>) -> crate::error::RwError {
    ErrorCode::InvalidInputSyntax(reason.into()).into()
}

fn constant(expr: &ExprImpl, argument: &str) -> Result<Datum> {
    expr.try_fold_const()
        .ok_or_else(|| invalid(format!("{argument} must be constant")))?
}

fn json_constant(expr: &ExprImpl, argument: &str) -> Result<JsonbVal> {
    match constant(expr, argument)? {
        Some(ScalarImpl::Jsonb(value)) => Ok(value),
        _ => Err(invalid(format!("{argument} must be non-null JSONB"))),
    }
}

fn text_constant(expr: &ExprImpl, argument: &str) -> Result<String> {
    match constant(expr, argument)? {
        Some(ScalarImpl::Utf8(value)) if !value.trim().is_empty() => Ok(value.into()),
        _ => Err(invalid(format!("{argument} must be a nonempty string"))),
    }
}

fn literal(value: Value) -> ExprImpl {
    Literal::new(Some(JsonbVal::from(value).into()), DataType::Jsonb).into()
}

pub(super) fn bind(mut inputs: Vec<ExprImpl>) -> Result<ExprImpl> {
    if inputs.len() != 5 {
        return Err(invalid("ai_decide expects exactly five arguments"));
    }
    for (index, ty) in [
        DataType::Jsonb,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Jsonb,
        DataType::Varchar,
    ]
    .into_iter()
    .enumerate()
    {
        inputs[index].cast_implicit_mut(&ty)?;
    }
    let questions = json_constant(&inputs[3], "ai_decide questions")?;
    let schema = QuestionSchema::parse(&questions).map_err(|e| invalid(e.to_string()))?;
    // Ship the validated constant to the executor, not an unevaluated expression.
    inputs[3] = Literal::new(Some(questions.into()), DataType::Jsonb).into();
    Ok(FunctionCall::new_unchecked(ExprType::AiDecide, inputs, schema.return_type()).into())
}

/// Constructors are evaluated at bind time and never issue HTTP requests.
pub(super) fn option(inputs: Vec<ExprImpl>) -> Result<ExprImpl> {
    let [label, description]: [_; 2] = inputs
        .try_into()
        .map_err(|_| invalid("ai_option expects label and description"))?;
    let label = text_constant(&label, "ai_option label")?;
    let description = match constant(&description, "ai_option description")? {
        None => Value::Null,
        Some(ScalarImpl::Utf8(value)) if !value.trim().is_empty() => Value::String(value.into()),
        _ => {
            return Err(invalid(
                "ai_option description must be a nonempty string or NULL",
            ));
        }
    };
    Ok(literal(json!({"label": label, "description": description})))
}

pub(super) fn question(kind: &str, inputs: Vec<ExprImpl>) -> Result<ExprImpl> {
    let expected = if kind == "noul" { 2 } else { 3 };
    if inputs.len() != expected {
        return Err(invalid(format!(
            "ai question constructor expects {expected} arguments"
        )));
    }
    let id = text_constant(&inputs[0], "question name")?;
    let instructions = text_constant(&inputs[1], "question instructions")?;
    let mut definition = json!({"type": kind, "instructions": instructions});
    if kind != "noul" {
        let Some(ScalarImpl::List(criteria)) = constant(&inputs[2], "question criteria")? else {
            return Err(invalid("question criteria must be a non-null array"));
        };
        if kind == "choice" {
            let mut options = Map::new();
            for item in criteria.iter() {
                let (label, description) = match item {
                    Some(ScalarRefImpl::Utf8(label)) => (label.to_owned(), Value::Null),
                    Some(ScalarRefImpl::Jsonb(value)) => {
                        let value = JsonbVal::from(value).take();
                        let object =
                            value.as_object().filter(|v| v.len() == 2).ok_or_else(|| {
                                invalid("choice options must be labels or ai_option values")
                            })?;
                        let label = object
                            .get("label")
                            .and_then(Value::as_str)
                            .ok_or_else(|| invalid("choice option requires a string label"))?;
                        let description = object
                            .get("description")
                            .ok_or_else(|| invalid("choice option requires a description"))?;
                        (label.to_owned(), description.clone())
                    }
                    _ => {
                        return Err(invalid(
                            "choice options must be non-null labels or ai_option values",
                        ));
                    }
                };
                if options.insert(label, description).is_some() {
                    return Err(invalid(format!("question {id:?}: duplicate choice label")));
                }
            }
            definition["criteria"] = Value::Object(options);
        } else {
            let levels = criteria
                .iter()
                .map(|item| match item {
                    Some(ScalarRefImpl::Utf8(value)) => Ok(Value::String(value.to_owned())),
                    _ => Err(invalid("score levels must be non-null strings")),
                })
                .collect::<Result<Vec<_>>>()?;
            definition["criteria"] = Value::Array(levels);
        }
    }
    let result = json!({id: definition});
    QuestionSchema::parse(&JsonbVal::from(result.clone())).map_err(|e| invalid(e.to_string()))?;
    Ok(literal(result))
}

pub(super) fn questions(inputs: Vec<ExprImpl>) -> Result<ExprImpl> {
    let mut questions = Map::new();
    for input in inputs {
        let value = json_constant(&input, "ai_questions argument")?.take();
        let object = value
            .as_object()
            .ok_or_else(|| invalid("ai_questions expects question definitions"))?;
        for (id, question) in object {
            if questions.insert(id.clone(), question.clone()).is_some() {
                return Err(invalid(format!(
                    "ai_questions: duplicate question name {id:?}"
                )));
            }
        }
    }
    let result = Value::Object(questions);
    QuestionSchema::parse(&JsonbVal::from(result.clone())).map_err(|e| invalid(e.to_string()))?;
    Ok(literal(result))
}
