// Copyright 2025 RisingWave Labs
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

use std::borrow::Cow;
use std::collections::HashMap;

use regex::{Captures, Regex};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use thiserror_ext::AsReport;

use super::{Result, RowEncoder};
use crate::sink::SinkError;
use crate::sink::encoder::SerTo;

pub enum TemplateEncoder {
    String(TemplateStringEncoder),
    RedisGeoKey(TemplateRedisGeoKeyEncoder),
    RedisGeoValue(TemplateRedisGeoValueEncoder),
    RedisPubSubKey(TemplateRedisPubSubKeyEncoder),
}
impl TemplateEncoder {
    pub fn new_string(schema: Schema, col_indices: Option<Vec<usize>>, template: String) -> Self {
        TemplateEncoder::String(TemplateStringEncoder::new(schema, col_indices, template))
    }

    pub fn new_geo_value(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        lat_name: &str,
        lon_name: &str,
    ) -> Result<Self> {
        Ok(TemplateEncoder::RedisGeoValue(
            TemplateRedisGeoValueEncoder::new(schema, col_indices, lat_name, lon_name)?,
        ))
    }

    pub fn new_geo_key(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        member_name: &str,
        template: String,
    ) -> Result<Self> {
        Ok(TemplateEncoder::RedisGeoKey(
            TemplateRedisGeoKeyEncoder::new(schema, col_indices, member_name, template)?,
        ))
    }

    pub fn new_pubsub_key(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        channel: Option<String>,
        channel_column: Option<String>,
    ) -> Result<Self> {
        Ok(TemplateEncoder::RedisPubSubKey(
            TemplateRedisPubSubKeyEncoder::new(schema, col_indices, channel, channel_column)?,
        ))
    }
}
impl RowEncoder for TemplateEncoder {
    type Output = TemplateEncoderOutput;

    fn schema(&self) -> &Schema {
        match self {
            TemplateEncoder::String(encoder) => &encoder.schema,
            TemplateEncoder::RedisGeoValue(encoder) => &encoder.schema,
            TemplateEncoder::RedisGeoKey(encoder) => &encoder.key_encoder.schema,
            TemplateEncoder::RedisPubSubKey(encoder) => &encoder.schema,
        }
    }

    fn col_indices(&self) -> Option<&[usize]> {
        match self {
            TemplateEncoder::String(encoder) => encoder.col_indices.as_deref(),
            TemplateEncoder::RedisGeoValue(encoder) => encoder.col_indices.as_deref(),
            TemplateEncoder::RedisGeoKey(encoder) => encoder.key_encoder.col_indices.as_deref(),
            TemplateEncoder::RedisPubSubKey(encoder) => encoder.col_indices.as_deref(),
        }
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        match self {
            TemplateEncoder::String(encoder) => Ok(TemplateEncoderOutput::String(
                encoder.encode_cols(row, col_indices)?,
            )),
            TemplateEncoder::RedisGeoValue(encoder) => encoder.encode_cols(row, col_indices),
            TemplateEncoder::RedisGeoKey(encoder) => encoder.encode_cols(row, col_indices),
            TemplateEncoder::RedisPubSubKey(encoder) => encoder.encode_cols(row, col_indices),
        }
    }
}
/// Encode a row according to a specified string template `user_id:{user_id}`.
/// Data is encoded to string with [`ToText`].
pub struct TemplateStringEncoder {
    field_name_to_index: HashMap<String, (usize, Field)>,
    col_indices: Option<Vec<usize>>,
    template: String,
    schema: Schema,
}

/// todo! improve the performance.
impl TemplateStringEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>, template: String) -> Self {
        let field_name_to_index = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name.clone(), (index, field.clone())))
            .collect();
        Self {
            field_name_to_index,
            col_indices,
            template,
            schema,
        }
    }

    pub fn check_string_format(format: &str, map: &HashMap<String, DataType>) -> Result<()> {
        // We will check if the string inside {} corresponds to a column name in rw.
        let re = Regex::new(r"(\\\})|(\\\{)|\{([^}]*)\}").unwrap();
        if !re.is_match(format) {
            return Err(SinkError::Redis(
                "Can't find {} in key_format or value_format".to_owned(),
            ));
        }
        for capture in re.captures_iter(format) {
            if let Some(inner_content) = capture.get(3)
                && !map.contains_key(inner_content.as_str())
            {
                return Err(SinkError::Redis(format!(
                    "Can't find field({:?}) in key_format or value_format",
                    inner_content.as_str()
                )));
            }
        }
        Ok(())
    }

    pub fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<String> {
        let s = self.template.clone();
        let re = Regex::new(r"(\\\})|(\\\{)|\{([^}]*)\}").unwrap();
        let col_indices: Vec<_> = col_indices.collect();
        let replaced = re.replace_all(s.as_ref(), |caps: &Captures<'_>| {
            if caps.get(1).is_some() {
                Cow::Borrowed("}")
            } else if caps.get(1).is_some() {
                Cow::Borrowed("{")
            } else if let Some(content) = caps.get(3) {
                let (idx, field) = self.field_name_to_index.get(content.as_str()).unwrap();
                if col_indices.contains(idx) {
                    let data = row.datum_at(*idx).to_text_with_type(&field.data_type);
                    Cow::Owned(data)
                } else {
                    Cow::Borrowed("")
                }
            } else {
                Cow::Borrowed("")
            }
        });
        Ok(replaced.to_string())
    }
}

pub struct TemplateRedisGeoValueEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    lat_col: usize,
    lon_col: usize,
}

impl TemplateRedisGeoValueEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        lat_name: &str,
        lon_name: &str,
    ) -> Result<Self> {
        let lat_col = schema
            .names_str()
            .iter()
            .position(|name| name == &lat_name)
            .ok_or_else(|| {
                SinkError::Redis(format!("Can't find lat column({}) in schema", lat_name))
            })?;
        let lon_col = schema
            .names_str()
            .iter()
            .position(|name| name == &lon_name)
            .ok_or_else(|| {
                SinkError::Redis(format!("Can't find lon column({}) in schema", lon_name))
            })?;
        Ok(Self {
            schema,
            col_indices,
            lat_col,
            lon_col,
        })
    }

    pub fn encode_cols(
        &self,
        row: impl Row,
        _col_indices: impl Iterator<Item = usize>,
    ) -> Result<TemplateEncoderOutput> {
        let lat = into_string_from_scalar(
            row.datum_at(self.lat_col)
                .ok_or_else(|| SinkError::Redis("lat is null".to_owned()))?,
        )?;
        let lon = into_string_from_scalar(
            row.datum_at(self.lon_col)
                .ok_or_else(|| SinkError::Redis("lon is null".to_owned()))?,
        )?;
        Ok(TemplateEncoderOutput::RedisGeoValue((lat, lon)))
    }
}

fn into_string_from_scalar(scalar: ScalarRefImpl<'_>) -> Result<String> {
    match scalar {
        ScalarRefImpl::Float32(ordered_float) => Ok(Into::<f32>::into(ordered_float).to_string()),
        ScalarRefImpl::Float64(ordered_float) => Ok(Into::<f64>::into(ordered_float).to_string()),
        ScalarRefImpl::Utf8(s) => Ok(s.to_owned()),
        _ => Err(SinkError::Encode(
            "Only f32 and f64 can convert to redis geo".to_owned(),
        )),
    }
}

pub struct TemplateRedisGeoKeyEncoder {
    key_encoder: TemplateStringEncoder,
    member_col: usize,
}

impl TemplateRedisGeoKeyEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        member_name: &str,
        template: String,
    ) -> Result<Self> {
        let member_col = schema
            .names_str()
            .iter()
            .position(|name| name == &member_name)
            .ok_or_else(|| {
                SinkError::Redis(format!(
                    "Can't find member column({}) in schema",
                    member_name
                ))
            })?;
        let key_encoder = TemplateStringEncoder::new(schema, col_indices, template);
        Ok(Self {
            key_encoder,
            member_col,
        })
    }

    pub fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<TemplateEncoderOutput> {
        let member = row
            .datum_at(self.member_col)
            .ok_or_else(|| SinkError::Redis("member is null".to_owned()))?
            .to_text()
            .clone();
        let key = self.key_encoder.encode_cols(row, col_indices)?;
        Ok(TemplateEncoderOutput::RedisGeoKey((key, member)))
    }
}

pub enum TemplateRedisPubSubKeyEncoderInner {
    PubSubName(String),
    PubSubColumnIndex(usize),
}
pub struct TemplateRedisPubSubKeyEncoder {
    inner: TemplateRedisPubSubKeyEncoderInner,
    schema: Schema,
    col_indices: Option<Vec<usize>>,
}

impl TemplateRedisPubSubKeyEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        channel: Option<String>,
        channel_column: Option<String>,
    ) -> Result<Self> {
        if let Some(channel) = channel {
            return Ok(Self {
                inner: TemplateRedisPubSubKeyEncoderInner::PubSubName(channel),
                schema,
                col_indices,
            });
        }
        if let Some(channel_column) = channel_column {
            let channel_column_index = schema
                .names_str()
                .iter()
                .position(|name| name == &channel_column)
                .ok_or_else(|| {
                    SinkError::Redis(format!(
                        "Can't find pubsub column({}) in schema",
                        channel_column
                    ))
                })?;
            return Ok(Self {
                inner: TemplateRedisPubSubKeyEncoderInner::PubSubColumnIndex(channel_column_index),
                schema,
                col_indices,
            });
        }
        Err(SinkError::Redis(
            "`channel` or `channel_column` must be set".to_owned(),
        ))
    }

    pub fn encode_cols(
        &self,
        row: impl Row,
        _col_indices: impl Iterator<Item = usize>,
    ) -> Result<TemplateEncoderOutput> {
        match &self.inner {
            TemplateRedisPubSubKeyEncoderInner::PubSubName(channel) => {
                Ok(TemplateEncoderOutput::RedisPubSubKey(channel.clone()))
            }
            TemplateRedisPubSubKeyEncoderInner::PubSubColumnIndex(pubsub_col) => {
                let pubsub_key = row
                    .datum_at(*pubsub_col)
                    .ok_or_else(|| SinkError::Redis("pubsub_key is null".to_owned()))?
                    .to_text()
                    .clone();
                Ok(TemplateEncoderOutput::RedisPubSubKey(pubsub_key))
            }
        }
    }
}

pub enum TemplateEncoderOutput {
    // String formatted according to the template
    String(String),
    // The value of redis's geospatial, including longitude and latitude
    RedisGeoValue((String, String)),
    // The key of redis's geospatial, including redis's key and member
    RedisGeoKey((String, String)),

    RedisPubSubKey(String),
}

impl TemplateEncoderOutput {
    pub fn into_string(self) -> Result<String> {
        match self {
            TemplateEncoderOutput::String(s) => Ok(s),
            TemplateEncoderOutput::RedisGeoKey(_) => Err(SinkError::Encode(
                "RedisGeoKey can't convert to string".to_owned(),
            )),
            TemplateEncoderOutput::RedisGeoValue(_) => Err(SinkError::Encode(
                "RedisGeoVelue can't convert to string".to_owned(),
            )),
            TemplateEncoderOutput::RedisPubSubKey(s) => Ok(s),
        }
    }
}

impl SerTo<String> for TemplateEncoderOutput {
    fn ser_to(self) -> Result<String> {
        match self {
            TemplateEncoderOutput::String(s) => Ok(s),
            TemplateEncoderOutput::RedisGeoKey(_) => Err(SinkError::Encode(
                "RedisGeoKey can't convert to string".to_owned(),
            )),
            TemplateEncoderOutput::RedisGeoValue(_) => Err(SinkError::Encode(
                "RedisGeoVelue can't convert to string".to_owned(),
            )),
            TemplateEncoderOutput::RedisPubSubKey(s) => Ok(s),
        }
    }
}

/// The enum of inputs to `RedisSinkPayloadWriter`
#[derive(Debug)]
pub enum RedisSinkPayloadWriterInput {
    // Json and String will be convert to string
    String(String),
    // The value of redis's geospatial, including longitude and latitude
    RedisGeoValue((String, String)),
    // The key of redis's geospatial, including redis's key and member
    RedisGeoKey((String, String)),
    RedisPubSubKey(String),
}

impl SerTo<RedisSinkPayloadWriterInput> for TemplateEncoderOutput {
    fn ser_to(self) -> Result<RedisSinkPayloadWriterInput> {
        match self {
            TemplateEncoderOutput::String(s) => Ok(RedisSinkPayloadWriterInput::String(s)),
            TemplateEncoderOutput::RedisGeoKey((lat, lon)) => {
                Ok(RedisSinkPayloadWriterInput::RedisGeoKey((lat, lon)))
            }
            TemplateEncoderOutput::RedisGeoValue((key, member)) => {
                Ok(RedisSinkPayloadWriterInput::RedisGeoValue((key, member)))
            }
            TemplateEncoderOutput::RedisPubSubKey(s) => {
                Ok(RedisSinkPayloadWriterInput::RedisPubSubKey(s))
            }
        }
    }
}

impl<T: SerTo<Vec<u8>>> SerTo<RedisSinkPayloadWriterInput> for T {
    default fn ser_to(self) -> Result<RedisSinkPayloadWriterInput> {
        let bytes = self.ser_to()?;
        Ok(RedisSinkPayloadWriterInput::String(
            String::from_utf8(bytes).map_err(|e| SinkError::Redis(e.to_report_string()))?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;

    #[test]
    fn test_template_format_validation() {
        // Create a schema with test columns
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "email".to_owned(),
            },
        ]);

        // Create a map of column names to their data types
        let mut map = HashMap::new();
        for field in schema.fields() {
            map.insert(field.name.clone(), field.data_type.clone());
        }

        // Test various template formats
        let valid_templates = vec![
            "user:{id}",
            "user:\\{{id}",
            "user:\\{{id}\\}",
            "user:\\{{id},{name}\\}",
            "user:\\{prefix{id},suffix{name}\\}",
            "user:\\{prefix{id},suffix{name},email:{email}\\}",
            "user:\\{nested\\{deeply{id}\\}\\}",
            "user:\\{outer\\{inner{id}\\},another{name}\\}",
            "user:\\{complex\\{structure\\{with{id}\\},and{name}\\},email:{email}\\}",
            "user:{id}{name}",
            "user:\\\\{id}",
            "user:\\\\\\{id}",
            "user:\\a{id}",
            "user:\\b{name}",
            "user:{id}{name}{email}",
        ];

        for template in valid_templates {
            // Validate the template format
            assert!(
                TemplateStringEncoder::check_string_format(template, &map).is_ok(),
                "Template '{}' should be valid",
                template
            );
        }

        // Test invalid templates
        let invalid_templates = vec![
            "user:no_braces",        // No braces
            "user:{invalid_column}", // Non-existent column
            "user:{id",              // Unclosed brace
            "user:id}",              // Unopened brace
            "sadsadsad{}qw4e2ewq21", // Empty braces
            "user:{}",
            "user:{\\id}",
        ];

        for template in invalid_templates {
            // Validate the template format
            assert!(
                TemplateStringEncoder::check_string_format(template, &map).is_err(),
                "Template '{}' should be invalid",
                template
            );
        }
    }

    #[test]
    fn test_template_encoding() {
        // Create a schema with test columns
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "email".to_owned(),
            },
        ]);

        // Test cases with different template formats
        let test_cases = vec![
            ("user:{id}", "user:123", vec![0]),
            ("user:\\{id\\}", "user:{id}", vec![0]),
            ("user:\\{id,name\\}", "user:{id,name}", vec![0, 1]),
            (
                "user:\\{prefix{id},suffix{name}\\}",
                "user:{prefix123,suffixJohn Doe}",
                vec![0, 1],
            ),
            (
                "user:\\{nested\\{deeply{id}\\}\\}",
                "user:{nested{deeply123}}",
                vec![0],
            ),
            (
                "user:\\{outer\\{inner{id}\\},another{name}\\}",
                "user:{outer{inner123},anotherJohn Doe}",
                vec![0, 1],
            ),
            ("user:{id}{name}", "user:123John Doe", vec![0, 1]),
            ("user:\\{id\\}{name}", "user:{id}John Doe", vec![0, 1]),
            ("user:\\\\{id}", "user:\\{id}", vec![0]),
            ("user:\\\\\\{id}", "user:\\\\{id}", vec![0]),
            ("user:\\a{id}", "user:\\a123", vec![0]),
            ("user:\\b{name}", "user:\\bJohn Doe", vec![1]),
            (
                "user:{id}{name}{email}",
                "user:123John Doejohn@example.com",
                vec![0, 1, 2],
            ),
        ];

        for (template, expected, col_indices) in test_cases {
            // Create an encoder with the template
            let encoder = TemplateStringEncoder::new(
                schema.clone(),
                Some(col_indices.clone()),
                template.to_owned(),
            );

            // Create a test row
            let row = OwnedRow::new(vec![
                Some(ScalarImpl::Int32(123)),
                Some(ScalarImpl::Utf8("John Doe".into())),
                Some(ScalarImpl::Utf8("john@example.com".into())),
            ]);

            // Encode the row
            let result = encoder.encode_cols(row, col_indices.into_iter()).unwrap();

            // Check the result
            assert_eq!(result, expected, "Template '{}' encoding failed", template);
        }
    }

    #[test]
    fn test_complex_nested_template() {
        // Create a schema with test columns
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".to_owned(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "email".to_owned(),
            },
        ]);

        // Create a map of column names to their data types
        let mut map = HashMap::new();
        for field in schema.fields() {
            map.insert(field.name.clone(), field.data_type.clone());
        }

        // Test a very complex nested template
        let complex_template = "user:\\{prefix{id},suffix{name},email:{email},nested\\{deeply{id}\\},outer\\{inner{name}\\}\\}";

        // Validate the template format
        assert!(TemplateStringEncoder::check_string_format(complex_template, &map).is_ok());

        // Create an encoder with the template
        let encoder = TemplateStringEncoder::new(
            schema.clone(),
            Some(vec![0, 1, 2]), // Include all columns
            complex_template.to_owned(),
        );

        // Create a test row
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int32(123)),
            Some(ScalarImpl::Utf8("John Doe".into())),
            Some(ScalarImpl::Utf8("john@example.com".into())),
        ]);

        // Encode the row
        let result = encoder.encode_cols(row, vec![0, 1, 2].into_iter()).unwrap();

        // Check that all column values are in the result
        assert_eq!(
            result,
            "user:{prefix123,suffixJohn Doe,email:john@example.com,nested{deeply123},outer{innerJohn Doe}}"
        );
    }
}
