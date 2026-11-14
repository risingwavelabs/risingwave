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

//! Fixed extraction schemas shared by the binder and executor.

use risingwave_common::config::AiDecideConfig;
use risingwave_common::types::{DataType, JsonbVal, StructType};

use crate::{ExprError, Result};

tokio::task_local! {
    static BUILD_CONFIG: AiDecideConfig;
}

/// Apply settings only while synchronously serializing or building expressions.
/// Streaming supplies the current job/global settings here, overriding the node's
/// creation-time settings. No scope survives into expression evaluation.
pub fn with_config<T>(config: AiDecideConfig, build: impl FnOnce() -> T) -> T {
    BUILD_CONFIG.sync_scope(config, build)
}

/// Resolve streaming settings while an executor is being constructed. The returned
/// executor retains the settings in its `AI_EXTRACT` expressions, not in a task scope.
pub async fn with_config_async<F: std::future::Future>(
    config: AiDecideConfig,
    build: F,
) -> F::Output {
    BUILD_CONFIG.scope(config, build).await
}

pub fn config() -> AiDecideConfig {
    config_override().unwrap_or_default()
}

pub(crate) fn config_override() -> Option<AiDecideConfig> {
    BUILD_CONFIG.try_with(Clone::clone).ok()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordField {
    pub name: String,
    pub list: bool,
}

#[derive(Debug, Clone)]
pub enum ExtractionKind {
    Entities(Vec<String>),
    Records {
        name: String,
        fields: Vec<RecordField>,
    },
}

#[derive(Debug, Clone)]
pub struct ExtractionSchema {
    pub kind: ExtractionKind,
    pub word_splitter: String,
}

impl ExtractionSchema {
    pub fn parse(spec: &JsonbVal) -> Result<Self> {
        let value = spec.clone().take();
        let object = value
            .as_object()
            .ok_or_else(|| invalid("expected an object"))?;
        let word_splitter = match value.get("word_splitter").and_then(|v| v.as_str()) {
            Some("whitespace") => "whitespace",
            Some("char") => "char",
            _ => return Err(invalid("word_splitter must be whitespace or char")),
        }
        .to_owned();
        if let Some(threshold) = value.get("threshold")
            && threshold
                .as_f64()
                .is_none_or(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
        {
            return Err(invalid("threshold must be a finite number in [0, 1]"));
        }
        let kind = match value.get("task").and_then(|v| v.as_str()) {
            Some("entities") => {
                if object.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "task" | "labels" | "word_splitter" | "threshold"
                    )
                }) {
                    return Err(invalid("unknown entity specification field"));
                }
                let labels = value
                    .get("labels")
                    .and_then(|v| v.as_array())
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| invalid("labels must be a nonempty array"))?;
                let mut seen = std::collections::HashSet::new();
                let labels = labels
                    .iter()
                    .map(|v| {
                        let label = name(v)?;
                        if !seen.insert(label.clone()) {
                            return Err(invalid("duplicate entity label"));
                        }
                        Ok(label)
                    })
                    .collect::<Result<Vec<_>>>()?;
                ExtractionKind::Entities(labels)
            }
            Some("records") => {
                if object.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "task"
                            | "record_name"
                            | "anchor"
                            | "fields"
                            | "word_splitter"
                            | "threshold"
                    )
                }) {
                    return Err(invalid("unknown record specification field"));
                }
                let record_name =
                    name(value.get("record_name").unwrap_or(&serde_json::Value::Null))?;
                let anchor = name(value.get("anchor").unwrap_or(&serde_json::Value::Null))?;
                let definitions = value
                    .get("fields")
                    .and_then(|v| v.as_array())
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| invalid("fields must be a nonempty array"))?;
                let mut seen = std::collections::HashSet::new();
                let fields = definitions
                    .iter()
                    .map(|v| {
                        let field = v
                            .as_object()
                            .ok_or_else(|| invalid("record field must be an object"))?;
                        if field
                            .keys()
                            .any(|key| !matches!(key.as_str(), "name" | "dtype" | "description"))
                        {
                            return Err(invalid("unknown record field attribute"));
                        }
                        let name = name(v.get("name").unwrap_or(&serde_json::Value::Null))?;
                        if name == "confidence" || !seen.insert(name.clone()) {
                            return Err(invalid("duplicate or reserved record field name"));
                        }
                        if v.get("description").is_some_and(|v| !v.is_string()) {
                            return Err(invalid("record field description must be a string"));
                        }
                        let list = match v.get("dtype") {
                            None => false,
                            Some(v) if v.as_str() == Some("str") => false,
                            Some(v) if v.as_str() == Some("list") => true,
                            _ => return Err(invalid("record dtype must be str or list")),
                        };
                        Ok(RecordField { name, list })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !seen.contains(&anchor) {
                    return Err(invalid("anchor must name a declared field"));
                }
                ExtractionKind::Records {
                    name: record_name,
                    fields,
                }
            }
            _ => return Err(invalid("task must be entities or records")),
        };
        Ok(Self {
            kind,
            word_splitter,
        })
    }

    pub fn task(&self) -> &'static str {
        match self.kind {
            ExtractionKind::Entities(_) => "entities",
            ExtractionKind::Records { .. } => "records",
        }
    }

    pub fn return_type(&self) -> DataType {
        match &self.kind {
            ExtractionKind::Entities(labels) => DataType::Struct(StructType::new(
                labels
                    .iter()
                    .map(|label| (label.clone(), DataType::list(span_type()))),
            )),
            ExtractionKind::Records { name, fields } => {
                let record = StructType::new(
                    fields
                        .iter()
                        .map(|field| {
                            (
                                field.name.clone(),
                                if field.list {
                                    DataType::list(span_type())
                                } else {
                                    span_type()
                                },
                            )
                        })
                        .chain(std::iter::once((
                            "confidence".to_owned(),
                            DataType::Float64,
                        ))),
                );
                DataType::Struct(StructType::new([(
                    name.clone(),
                    DataType::list(DataType::Struct(record)),
                )]))
            }
        }
    }
}

pub fn span_type() -> DataType {
    DataType::Struct(StructType::new([
        ("text", DataType::Varchar),
        ("start", DataType::Int32),
        ("end", DataType::Int32),
        ("confidence", DataType::Float64),
    ]))
}

fn name(value: &serde_json::Value) -> Result<String> {
    value
        .as_str()
        .filter(|v| !v.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| invalid("names must be nonempty strings"))
}

fn invalid(reason: impl Into<String>) -> ExprError {
    ExprError::InvalidParam {
        name: "ai_extract specification",
        reason: reason.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn fixed_entity_and_record_types() {
        let entities = ExtractionSchema::parse(
            &json!({"task":"entities","labels":["person","company"],"word_splitter":"char"}).into(),
        )
        .unwrap();
        let ty = entities.return_type();
        let fields: Vec<_> = ty.as_struct().iter().collect();
        assert_eq!(fields[0], ("person", &DataType::list(span_type())));
        assert_eq!(fields[1].0, "company");
        let records = ExtractionSchema::parse(&json!({"task":"records","record_name":"sale","anchor":"product","fields":[{"name":"product"},{"name":"prices","dtype":"list"}],"word_splitter":"whitespace"}).into()).unwrap();
        let ty = records.return_type();
        let row = ty
            .as_struct()
            .iter()
            .next()
            .unwrap()
            .1
            .as_list()
            .elem()
            .as_struct();
        assert_eq!(
            row.names().collect::<Vec<_>>(),
            vec!["product", "prices", "confidence"]
        );
    }

    #[test]
    fn rejects_unsupported_and_ambiguous_specs() {
        let base = json!({"task":"entities","labels":["person"],"word_splitter":"char"});
        for (key, val) in [
            ("task", json!("other")),
            ("labels", json!([])),
            ("labels", json!(["x", "x"])),
            ("labels", json!([""])),
            ("word_splitter", json!("other")),
            ("threshold", json!(true)),
            ("threshold", json!(1.1)),
            ("model", json!("override")),
        ] {
            let mut value = base.clone();
            value[key] = val;
            assert!(ExtractionSchema::parse(&value.into()).is_err());
        }
        let base = json!({"task":"records","record_name":"sale","anchor":"product","fields":[{"name":"product"}],"word_splitter":"char"});
        for (key, val) in [
            ("anchor", json!("absent")),
            ("record_name", json!("")),
            ("fields", json!([])),
            ("fields", json!([{"name":"confidence"}])),
            ("fields", json!([{"name":"product","dtype":"number"}])),
            ("fields", json!([{"name":"product","description":false}])),
            ("fields", json!([{"name":"product"},{"name":"product"}])),
        ] {
            let mut value = base.clone();
            value[key] = val;
            assert!(ExtractionSchema::parse(&value.into()).is_err());
        }
    }
}
