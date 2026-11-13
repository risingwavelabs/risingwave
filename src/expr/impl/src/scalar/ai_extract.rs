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

//! Strict wire decoding for typed extraction results. HTTP execution is shared.

use risingwave_common::types::{Datum, F64, JsonbVal, ListValue, StructValue};
use risingwave_expr::scalar::ai_extract::{ExtractionKind, ExtractionSchema, span_type};
use risingwave_expr::{ExprError, Result};
use serde_json::{Map, Value};

fn invalid(reason: &str) -> ExprError {
    // Remote data can contain private text; never echo names, values or payloads.
    ExprError::Custom(format!("ai_extract: {reason}"))
}

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid("expected an object"))
}

fn confidence(value: &Value) -> Result<F64> {
    value
        .as_f64()
        .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
        .map(Into::into)
        .ok_or_else(|| invalid("invalid confidence"))
}

struct Spans<'a> {
    text: &'a str,
    // Map Unicode code points to UTF-8 byte boundaries once for the response.
    boundaries: Vec<usize>,
}

impl<'a> Spans<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            boundaries: text
                .char_indices()
                .map(|(i, _)| i)
                .chain(std::iter::once(text.len()))
                .collect(),
        }
    }

    fn decode(&self, value: &Value) -> Result<StructValue> {
        let fields = object(value)?;
        if fields.len() != 4
            || !["text", "start", "end", "confidence"]
                .iter()
                .all(|key| fields.contains_key(*key))
        {
            return Err(invalid("invalid span fields"));
        }
        let start = value["start"]
            .as_u64()
            .and_then(|v| i32::try_from(v).ok())
            .ok_or_else(|| invalid("invalid span start"))?;
        let end = value["end"]
            .as_u64()
            .and_then(|v| i32::try_from(v).ok())
            .ok_or_else(|| invalid("invalid span end"))?;
        if start >= end || end as usize >= self.boundaries.len() {
            return Err(invalid("span offsets outside content"));
        }
        let text = value["text"]
            .as_str()
            .ok_or_else(|| invalid("invalid span text"))?;
        if text != &self.text[self.boundaries[start as usize]..self.boundaries[end as usize]] {
            return Err(invalid("span text does not match content"));
        }
        Ok(StructValue::new(vec![
            Some(text.to_owned().into()),
            Some(start.into()),
            Some(end.into()),
            Some(confidence(&value["confidence"])?.into()),
        ]))
    }

    fn list(&self, value: &Value) -> Result<ListValue> {
        let values = value
            .as_array()
            .ok_or_else(|| invalid("expected a span array"))?;
        let values = values
            .iter()
            .map(|value| self.decode(value).map(|span| Some(span.into())))
            .collect::<Result<Vec<Datum>>>()?;
        Ok(ListValue::from_datum_iter(&span_type(), values))
    }
}

pub(super) fn decode_response(
    response: JsonbVal,
    schema: &ExtractionSchema,
    content: &str,
) -> Result<StructValue> {
    let response = response.take();
    if response["task"].as_str() != Some(schema.task())
        || response["offset_unit"].as_str() != Some("unicode_code_point")
        || response["end_exclusive"].as_bool() != Some(true)
        || response["word_splitter"].as_str() != Some(schema.word_splitter.as_str())
    {
        return Err(invalid(
            "response metadata does not match extraction protocol",
        ));
    }
    let result = object(&response["result"])?;
    let spans = Spans::new(content);
    match &schema.kind {
        ExtractionKind::Entities(labels) => {
            if result.len() != 1 {
                return Err(invalid("invalid entity result fields"));
            }
            let entities = object(
                result
                    .get("entities")
                    .ok_or_else(|| invalid("missing entities"))?,
            )?;
            if entities.len() != labels.len() {
                return Err(invalid("entity labels do not match specification"));
            }
            let fields = labels
                .iter()
                .map(|label| {
                    let value = entities
                        .get(label)
                        .ok_or_else(|| invalid("missing entity label"))?;
                    Ok(Some(spans.list(value)?.into()))
                })
                .collect::<Result<Vec<Datum>>>()?;
            Ok(StructValue::new(fields))
        }
        ExtractionKind::Records { name, fields } => {
            if result.len() != 1 {
                return Err(invalid("invalid record result fields"));
            }
            let records = result
                .get(name)
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("missing record array"))?;
            let records = records
                .iter()
                .map(|record| {
                    let values = object(record)?;
                    if values.len() != fields.len() + 1 {
                        return Err(invalid("invalid record fields"));
                    }
                    let mut row = fields
                        .iter()
                        .map(|field| {
                            let value = values
                                .get(&field.name)
                                .ok_or_else(|| invalid("missing record field"))?;
                            if field.list {
                                Ok(Some(spans.list(value)?.into()))
                            } else if value.is_null() {
                                Ok(None)
                            } else {
                                Ok(Some(spans.decode(value)?.into()))
                            }
                        })
                        .collect::<Result<Vec<Datum>>>()?;
                    row.push(Some(
                        confidence(
                            values
                                .get("confidence")
                                .ok_or_else(|| invalid("missing record confidence"))?,
                        )?
                        .into(),
                    ));
                    Ok(Some(StructValue::new(row).into()))
                })
                .collect::<Result<Vec<Datum>>>()?;
            let ty = schema.return_type();
            let record_type = ty.as_struct().iter().next().unwrap().1.as_list().elem();
            Ok(StructValue::new(vec![Some(
                ListValue::from_datum_iter(record_type, records).into(),
            )]))
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::Scalar;
    use serde_json::json;

    use super::*;

    fn entities() -> ExtractionSchema {
        ExtractionSchema::parse(
            &json!({"task":"entities", "labels":["person","company"],"word_splitter":"char"})
                .into(),
        )
        .unwrap()
    }

    fn response(task: &str, result: Value) -> Value {
        json!({"task":task,"offset_unit":"unicode_code_point","end_exclusive":true,"word_splitter":"char","result":result})
    }

    #[test]
    fn unicode_overlaps_order_and_empty_arrays() {
        let body = response(
            "entities",
            json!({"entities":{"person":[
            {"text":"张三","start":1,"end":3,"confidence":0.8},
            {"text":"张","start":1,"end":2,"confidence":1.0}],"company":[]}}),
        );
        let result = decode_response(body.clone().into(), &entities(), "🙂张三").unwrap();
        let expected = JsonbVal::from(
            json!({"person":[{"text":"张三","start":1,"end":3,"confidence":0.8},{"text":"张","start":1,"end":2,"confidence":1.0}],"company":[]}),
        );
        assert_eq!(
            Some(result.into()),
            expected
                .as_scalar_ref()
                .to_datum(&entities().return_type())
                .unwrap()
        );
        for (pointer, bad) in [
            ("/result/entities/person/0/start", json!(4)),
            ("/result/entities/person/0/end", json!(1)),
            ("/result/entities/person/0/start", json!(-1)),
            ("/result/entities/person/0/end", json!(3.5)),
            ("/result/entities/person/0/text", json!("private response")),
            ("/result/entities/person/0/confidence", json!(1.1)),
            ("/result/entities/company", Value::Null),
            ("/offset_unit", json!("byte")),
            ("/word_splitter", json!("whitespace")),
            ("/end_exclusive", json!(false)),
        ] {
            let mut invalid = body.clone();
            *invalid.pointer_mut(pointer).unwrap() = bad;
            let error = decode_response(invalid.into(), &entities(), "🙂张三").unwrap_err();
            assert!(!error.to_string().contains("private response"));
        }
        let mut missing = body.clone();
        missing["result"]["entities"]
            .as_object_mut()
            .unwrap()
            .remove("company");
        assert!(decode_response(missing.into(), &entities(), "🙂张三").is_err());
        let mut extra = body;
        extra["result"]["entities"]["unknown"] = json!([]);
        assert!(decode_response(extra.into(), &entities(), "🙂张三").is_err());
    }

    #[test]
    fn records_keep_null_fields_and_typed_lists() {
        let schema = ExtractionSchema::parse(&json!({"task":"records","record_name":"sale","anchor":"product","fields":[{"name":"product"},{"name":"price"},{"name":"tags","dtype":"list"}],"word_splitter":"char"}).into()).unwrap();
        let body = response(
            "records",
            json!({"sale":[{"product":{"text":"apple","start":0,"end":5,"confidence":0.9},"price":null,"tags":[],"confidence":0.9}]}),
        );
        assert!(decode_response(body.clone().into(), &schema, "apple").is_ok());
        assert!(
            decode_response(
                response("records", json!({"sale":[]})).into(),
                &schema,
                "apple"
            )
            .is_ok()
        );
        for pointer in [
            "/result/sale/0/product",
            "/result/sale/0/price",
            "/result/sale/0/tags",
            "/result/sale/0/confidence",
        ] {
            let mut malformed = body.clone();
            let key = pointer.rsplit('/').next().unwrap();
            malformed["result"]["sale"][0]
                .as_object_mut()
                .unwrap()
                .remove(key);
            assert!(decode_response(malformed.into(), &schema, "apple").is_err());
        }
        let mut invalid = body;
        invalid["result"]["sale"][0]["tags"] = Value::Null;
        assert!(decode_response(invalid.into(), &schema, "apple").is_err());
    }
}
