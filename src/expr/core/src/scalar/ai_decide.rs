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

//! Fixed question schemas shared by the binder and executor.

use risingwave_common::types::{DataType, JsonbVal, MapType, StructType};
use serde_json::Value;

use crate::{ExprError, Result};

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
                .is_none_or(|v| !is_description(v))
            {
                return Err(error(
                    "instructions must be a string, object, array or null",
                ));
            }
            let kind = match definition.get("type").and_then(|v| v.as_str()) {
                Some("noul") => {
                    if let Some(criteria) = definition.get("criteria").filter(|v| !v.is_null()) {
                        let criteria = criteria.as_object().ok_or_else(|| {
                            error("probability criteria must be an object or null")
                        })?;
                        for (label, description) in criteria {
                            if !matches!(label.as_str(), "true" | "false") {
                                return Err(error(
                                    "probability criteria keys must be true or false",
                                ));
                            }
                            if !is_description(description) {
                                return Err(error(
                                    "probability descriptions must be strings, objects, arrays or null",
                                ));
                            }
                        }
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
                        if label.trim().is_empty() || !is_description(description) {
                            return Err(error(
                                "option labels must be nonempty; descriptions must be strings, objects, arrays or null",
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
                        if !is_description(description) {
                            return Err(error(
                                "score levels must be strings, objects, arrays or null",
                            ));
                        }
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

// EntryType constrains the outer shape, while nested JSON content is passed through.
fn is_description(value: &Value) -> bool {
    matches!(
        value,
        Value::String(_) | Value::Object(_) | Value::Array(_) | Value::Null
    )
}

fn invalid_questions(reason: impl Into<String>) -> ExprError {
    ExprError::InvalidParam {
        name: "ai_decide questions",
        reason: reason.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

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
            r#"{"q":{"type":"noul"}}"#,
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
    fn structured_descriptions_preserve_result_types() {
        for description in [
            json!("Question?"),
            json!(""),
            json!({"question": "Resolved?", "examples": [true, 42, null]}),
            json!(["Resolved?", {"minimum": 1}, false, null]),
            json!({}),
            json!([]),
            Value::Null,
        ] {
            let questions = json!({
                "probability": {"type": "noul", "instructions": description,
                    "criteria": {"true": description, "false": description}},
                "choice": {"type": "choice", "instructions": description,
                    "criteria": {"yes": description, "no": description}},
                "score": {"type": "score", "instructions": description,
                    "criteria": [description, "Another level"]}
            });
            let schema = QuestionSchema::parse(&JsonbVal::from(questions)).unwrap();
            let plain = QuestionSchema::parse(&JsonbVal::from(json!({
                "probability": {"type": "noul", "instructions": "Question?"},
                "choice": {"type": "choice", "instructions": "Question?",
                    "criteria": {"yes": "Yes", "no": "No"}},
                "score": {"type": "score", "instructions": "Question?",
                    "criteria": ["First", "Second"]}
            })))
            .unwrap();
            assert_eq!(schema.return_type(), plain.return_type());
        }
        for criteria in [
            json!(null),
            json!({}),
            json!({"true": "Yes"}),
            json!({"false": "No"}),
        ] {
            QuestionSchema::parse(&JsonbVal::from(json!({
                "q": {"type": "noul", "instructions": null, "criteria": criteria}
            })))
            .unwrap();
        }
    }

    #[test]
    fn structured_descriptions_still_reject_invalid_shapes() {
        for definition in [
            json!({"type": "noul", "instructions": true}),
            json!({"type": "noul", "instructions": 42}),
            json!({"type": "noul", "instructions": [], "criteria": []}),
            json!({"type": "noul", "instructions": {}, "criteria": {"yes": "Yes"}}),
            json!({"type": "noul", "instructions": null, "criteria": {"true": true}}),
            json!({"type": "noul", "instructions": null, "criteria": {"false": 0}}),
            json!({"type": "choice", "instructions": {}, "criteria": {"": {}}}),
            json!({"type": "choice", "instructions": [], "criteria": {"yes": false}}),
            json!({"type": "score", "instructions": {}, "criteria": [null, 42]}),
            json!({"type": "score", "instructions": [], "criteria": [null, false]}),
            json!({"type": "score", "instructions": null, "criteria": [{"same": 1}, {"same": 1}]}),
        ] {
            assert!(
                QuestionSchema::parse(&JsonbVal::from(json!({"q": definition}))).is_err(),
                "{definition}"
            );
        }
    }
}
