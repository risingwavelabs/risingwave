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

use core::any::Any;
use std::collections::HashSet;

use regex::Regex;
use risingwave_common::{catalog::Schema, types::ScalarRefImpl};
use risingwave_common::row::Row;
use risingwave_common::types::ToText;
use crate::sink::encoder::SerTo;

use super::{Result, RowEncoder};
use crate::sink::SinkError;

pub enum TemplateEncoder{
    String(TemplateStringEncoder),
    RedisGeo(TemplateRedisGeoEncoder),
}
impl TemplateEncoder {
    pub fn new_string(schema: Schema, col_indices: Option<Vec<usize>>, template: String) -> Self {
        TemplateEncoder::String(TemplateStringEncoder::new(schema, col_indices, template))
    }

    pub fn new_geo(schema: Schema, col_indices: Option<Vec<usize>>, lat_name: &str, lon_name: &str, member_name: &str) -> Result<Self> {
        Ok(TemplateEncoder::RedisGeo(TemplateRedisGeoEncoder::new(schema, col_indices, lat_name, lon_name, member_name)?))
    }
}
impl RowEncoder for TemplateEncoder {
    type Output = TemplateEncoderOutput;

    fn schema(&self) -> &Schema {
        match self {
            TemplateEncoder::String(encoder) => &encoder.schema,
            TemplateEncoder::RedisGeo(encoder) => &encoder.schema,
        }
    }

    fn col_indices(&self) -> Option<&[usize]> {
        match self {
            TemplateEncoder::String(encoder) => encoder.col_indices.as_deref(),
            TemplateEncoder::RedisGeo(encoder) => encoder.col_indices.as_deref(),
        }
    }

    fn encode_cols(
            &self,
            row: impl Row,
            col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        match self {
            TemplateEncoder::String(encoder) => Ok(TemplateEncoderOutput::String(encoder.encode_cols(row, col_indices)?)),
            TemplateEncoder::RedisGeo(encoder) => encoder.encode_cols(row, col_indices),
        }
    }
}
/// Encode a row according to a specified string template `user_id:{user_id}`.
/// Data is encoded to string with [`ToText`].
pub struct TemplateStringEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    template: String,
}

/// todo! improve the performance.
impl TemplateStringEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>, template: String) -> Self {
        Self {
            schema,
            col_indices,
            template,
        }
    }

    pub fn check_string_format(format: &str, set: &HashSet<String>) -> Result<()> {
        // We will check if the string inside {} corresponds to a column name in rw.
        // In other words, the content within {} should exclusively consist of column names from rw,
        // which means '{{column_name}}' or '{{column_name1},{column_name2}}' would be incorrect.
        let re = Regex::new(r"\{([^}]*)\}").unwrap();
        if !re.is_match(format) {
            return Err(SinkError::Redis(
                "Can't find {} in key_format or value_format".to_owned(),
            ));
        }
        for capture in re.captures_iter(format) {
            if let Some(inner_content) = capture.get(1)
                && !set.contains(inner_content.as_str())
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
        let mut s = self.template.clone();

        for idx in col_indices {
            let field = &self.schema[idx];
            let name = &field.name;
            let data = row.datum_at(idx);
            // TODO: timestamptz ToText also depends on TimeZone
            s = s.replace(
                &format!("{{{}}}", name),
                &data.to_text_with_type(&field.data_type),
            );
        }
        Ok(s)
    }
}

pub struct  TemplateRedisGeoEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    lat_col: usize,
    lon_col: usize,
    member_col: usize,
}

impl TemplateRedisGeoEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>, lat_name: &str, lon_name: &str, member_name: &str) -> Result<Self> {
        let lat_col = schema
            .names_str()
            .iter()
            .position(|name| name == &lat_name)
            .ok_or_else(|| SinkError::Redis(format!("Can't find lat column({}) in schema", lat_name)))?;
        let lon_col = schema
            .names_str()
            .iter()
            .position(|name| name == &lon_name)
            .ok_or_else(|| SinkError::Redis(format!("Can't find lon column({}) in schema", lon_name)))?;
        let member_col = schema
            .names_str()
            .iter()
            .position(|name| name == &member_name)
            .ok_or_else(|| SinkError::Redis(format!("Can't find member column({}) in schema", member_name)))?;
        Ok(Self {
            schema,
            col_indices,
            lat_col,
            lon_col,
            member_col,
        })
    }

    pub fn encode_cols(
        &self,
        row: impl Row,
        _col_indices: impl Iterator<Item = usize>,
    ) -> Result<TemplateEncoderOutput> {
        let lat = into_string_from_scalar(row.datum_at(self.lat_col).ok_or_else(|| SinkError::Redis("lat is null".to_owned()))?)?;
        let lon = into_string_from_scalar(row.datum_at(self.lon_col).ok_or_else(|| SinkError::Redis("lon is null".to_owned()))?)?;
        let member = row.datum_at(self.member_col).ok_or_else(|| SinkError::Redis("member is null".to_owned()))?.to_text();
        Ok(TemplateEncoderOutput::RedisGeo((lat, lon , member)))
    }
}

fn into_string_from_scalar(scalar: ScalarRefImpl<'_>) -> Result<String> {
    match scalar {
        ScalarRefImpl::Float32(ordered_float) => Ok(Into::<f32>::into(ordered_float).to_string()),
        ScalarRefImpl::Float64(ordered_float) => Ok(Into::<f64>::into(ordered_float).to_string()),
        _ => Err(SinkError::Encode("Only f32 and f64 can convert to redis geo".to_owned())),
    }
}

pub enum TemplateEncoderOutput {
    String(String),
    RedisGeo((String,String,String)),
}

impl TemplateEncoderOutput {
    pub fn into_string(self) -> Result<String> {
        match self {
            TemplateEncoderOutput::String(s) => Ok(s),
            TemplateEncoderOutput::RedisGeo((_, _,_)) => Err(SinkError::Encode("RedisGeo can't convert to string".to_owned())),
        }
    }
}

impl SerTo<String> for TemplateEncoderOutput {
    fn ser_to(self) -> Result<String> {
        match self {
            TemplateEncoderOutput::String(s) => Ok(s),
            TemplateEncoderOutput::RedisGeo((_, _,_)) => Err(SinkError::Encode("RedisGeo can't convert to string".to_owned())),
        }
    }
}

pub enum RedisEncoderOutput {
    String(String),
    RedisGeo((String,String,String)),
}

impl SerTo<RedisEncoderOutput> for TemplateEncoderOutput {
    fn ser_to(self) -> Result<RedisEncoderOutput> {
        match self {
            TemplateEncoderOutput::String(s) => Ok(RedisEncoderOutput::String(s)),
            TemplateEncoderOutput::RedisGeo((lat, lon, member)) => Ok(RedisEncoderOutput::RedisGeo((lat, lon, member))),
        }
    }
}

impl<T: SerTo<Vec<u8>>> SerTo<RedisEncoderOutput> for T {
    default fn ser_to(self) -> Result<RedisEncoderOutput> {
        let bytes = self.ser_to()?;
        Ok(RedisEncoderOutput::String(String::from_utf8(bytes).map_err(|e| SinkError::Redis(e.to_string()))?))
    }
}
