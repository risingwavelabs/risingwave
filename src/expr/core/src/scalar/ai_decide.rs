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

//! Fixed question schemas and execution options shared by the binder and executor.

use risingwave_common::types::{DataType, JsonbVal, MapType, StructType};

use crate::{ExprError, Result};

pub const DEFAULT_MAX_CONCURRENCY: usize = 8;
pub const MAX_CONCURRENCY_LIMIT: usize = 256;

#[derive(Debug, Clone)]
pub enum QuestionKind {
    Noul,
    Choice(Vec<String>),
    Score(usize),
}

#[derive(Debug, Clone)]
pub struct Question {
    pub id: String,
    pub kind: QuestionKind,
}

#[derive(Debug, Clone)]
pub struct QuestionSchema(pub Vec<Question>);

impl QuestionSchema {
    pub fn parse(questions: &JsonbVal) -> Result<Self> {
        let value = questions.clone().take();
        let object = value.as_object().filter(|v| !v.is_empty()).ok_or_else(|| {
            invalid_questions("expected a nonempty object of fixed question definitions")
        })?;
        let mut result = Vec::with_capacity(object.len());
        for (id, definition) in object {
            let error = |reason: &str| invalid_questions(format!("question {id:?}: {reason}"));
            if id.trim().is_empty() {
                return Err(error("name must not be empty"));
            }
            let definition = definition
                .as_object()
                .ok_or_else(|| error("expected an object"))?;
            if definition
                .keys()
                .any(|k| !matches!(k.as_str(), "type" | "instructions" | "criteria"))
            {
                return Err(error(
                    "unknown field; expected type, instructions or criteria",
                ));
            }
            if definition
                .get("instructions")
                .and_then(|v| v.as_str())
                .is_none_or(|s| s.trim().is_empty())
            {
                return Err(error("instructions must be a nonempty string"));
            }
            let kind = match definition.get("type").and_then(|v| v.as_str()) {
                Some("noul") => {
                    if definition.contains_key("criteria") {
                        return Err(error("probability questions do not accept criteria"));
                    }
                    QuestionKind::Noul
                }
                Some("choice") => {
                    let criteria = definition
                        .get("criteria")
                        .and_then(|v| v.as_object())
                        .filter(|v| (1..=255).contains(&v.len()))
                        .ok_or_else(|| error("choice requires 1 to 255 named options"))?;
                    for (label, description) in criteria {
                        if label.trim().is_empty()
                            || !(description.is_null()
                                || description.as_str().is_some_and(|s| !s.trim().is_empty()))
                        {
                            return Err(error(
                                "option labels must be nonempty; descriptions must be nonempty strings or null",
                            ));
                        }
                    }
                    QuestionKind::Choice(criteria.keys().cloned().collect())
                }
                Some("score") => {
                    let criteria = definition
                        .get("criteria")
                        .and_then(|v| v.as_array())
                        .filter(|v| (2..=10).contains(&v.len()))
                        .ok_or_else(|| {
                            error("score requires 2 to 10 ordered level descriptions")
                        })?;
                    let mut seen = std::collections::HashSet::new();
                    for description in criteria {
                        let description = description
                            .as_str()
                            .filter(|s| !s.trim().is_empty())
                            .ok_or_else(|| error("score levels must be nonempty strings"))?;
                        if !seen.insert(description) {
                            return Err(error("score levels must be unique"));
                        }
                    }
                    QuestionKind::Score(criteria.len())
                }
                _ => return Err(error("type must be noul, choice or score")),
            };
            result.push(Question {
                id: id.clone(),
                kind,
            });
        }
        Ok(Self(result))
    }

    pub fn return_type(&self) -> DataType {
        DataType::Struct(StructType::new(self.0.iter().map(|question| {
            let ty = match &question.kind {
                QuestionKind::Noul => DataType::Float64,
                QuestionKind::Choice(_) => DataType::Struct(StructType::new([
                    ("choice", DataType::Varchar),
                    ("confidence", DataType::Float64),
                    (
                        "probabilities",
                        MapType::from_kv(DataType::Varchar, DataType::Float64).into(),
                    ),
                ])),
                QuestionKind::Score(_) => DataType::Struct(StructType::new([
                    ("score", DataType::Float64),
                    ("confidence", DataType::Float64),
                    ("probabilities", DataType::list(DataType::Float64)),
                ])),
            };
            (question.id.clone(), ty)
        })))
    }
}

fn invalid_questions(reason: impl Into<String>) -> ExprError {
    ExprError::InvalidParam {
        name: "ai_decide questions",
        reason: reason.into().into(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AiDecideOptions {
    pub max_concurrency: usize,
}

impl Default for AiDecideOptions {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }
}

impl AiDecideOptions {
    pub fn parse(options: &JsonbVal) -> Result<Self> {
        let value = options.clone().take();
        let object = value
            .as_object()
            .ok_or_else(|| invalid_options("options must be a JSON object"))?;
        if object.keys().any(|key| key != "max_concurrency") {
            return Err(invalid_options("unknown option; expected max_concurrency"));
        }
        let Some(value) = object.get("max_concurrency") else {
            return Ok(Self::default());
        };
        let max_concurrency = value
            .as_u64()
            .filter(|value| (1..=MAX_CONCURRENCY_LIMIT as u64).contains(value))
            .ok_or_else(|| invalid_options("max_concurrency must be an integer from 1 to 256"))?;
        Ok(Self {
            max_concurrency: max_concurrency as usize,
        })
    }
}

fn invalid_options(reason: &'static str) -> ExprError {
    // Do not include the supplied JSON, including unknown keys, in errors.
    ExprError::InvalidParam {
        name: "ai_decide options",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_questions_infer_named_fields_and_reject_invalid_definitions() {
        let schema = QuestionSchema::parse(&r#"{
            "refund":{"type":"noul","instructions":"Refund requested?"},
            "team":{"type":"choice","instructions":"Which team?","criteria":{"billing":"Payments","tech":null}},
            "urgency":{"type":"score","instructions":"How urgent?","criteria":["Routine","Blocking"]}
        }"#.parse().unwrap()).unwrap();
        let ty = schema.return_type();
        let fields: Vec<_> = ty.as_struct().iter().collect();
        assert_eq!(fields[0], ("refund", &DataType::Float64));
        assert_eq!(fields[1].0, "team");
        assert_eq!(
            fields[1].1.as_struct().names().collect::<Vec<_>>(),
            vec!["choice", "confidence", "probabilities"]
        );
        assert_eq!(fields[2].0, "urgency");
        for json in [
            "null",
            "[]",
            "{}",
            r#"{"":{"type":"noul","instructions":"Question?"}}"#,
            r#"{"q":{"type":"noul","instructions":" "}}"#,
            r#"{"q":{"type":"text","instructions":"Question?"}}"#,
            r#"{"q":{"type":"noul","instructions":"Question?","unknown":true}}"#,
            r#"{"q":{"type":"choice","instructions":"Question?","criteria":{}}}"#,
            r#"{"q":{"type":"choice","instructions":"Question?","criteria":{"a":42}}}"#,
            r#"{"q":{"type":"score","instructions":"Question?","criteria":["same","same"]}}"#,
            r#"{"q":{"type":"score","instructions":"Question?","criteria":["only"]}}"#,
        ] {
            assert!(
                QuestionSchema::parse(&json.parse().unwrap()).is_err(),
                "{json}"
            );
        }
    }

    #[test]
    fn validates_bounded_options_without_echoing_values() {
        for (json, expected) in [
            ("{}", 8),
            (r#"{"max_concurrency":1}"#, 1),
            (r#"{"max_concurrency":16}"#, 16),
            (r#"{"max_concurrency":256}"#, 256),
        ] {
            assert_eq!(
                AiDecideOptions::parse(&json.parse().unwrap())
                    .unwrap()
                    .max_concurrency,
                expected
            );
        }
        for json in [
            "null",
            "[]",
            "8",
            r#"{"max_concurrency":null}"#,
            r#"{"max_concurrency":0}"#,
            r#"{"max_concurrency":-1}"#,
            r#"{"max_concurrency":257}"#,
            r#"{"max_concurrency":1.5}"#,
            r#"{"max_concurrency":"test-secret"}"#,
            r#"{"test-secret":8}"#,
        ] {
            let error = AiDecideOptions::parse(&json.parse().unwrap()).unwrap_err();
            assert!(!error.to_string().contains("test-secret"));
        }
    }
}
