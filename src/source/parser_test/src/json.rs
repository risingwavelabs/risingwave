// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use rand::Rng;
use risingwave_common::array::{Row, StructValue};
use risingwave_common::try_match_expand;
use risingwave_common::types::decimal::ToPrimitive;
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_source::{SourceColumnDesc, SourceFormat};
use serde_json::{Map, Number, Value};

use crate::gen_datum;

pub struct JsonGen<R: Rng> {
    rng: R,
}

impl<R: Rng> JsonGen<R> {
    pub fn new(rng: R) -> Self {
        Self { rng }
    }

    /// Returns the datum and the converted JSON string.
    pub fn gen_record(&mut self, schema: &Vec<SourceColumnDesc>) -> (Row, String) {
        let values = schema
            .iter()
            .map(|f| gen_datum(&mut self.rng, &f.data_type))
            .collect();
        let row = Row::new(values);
        let record = row
            .clone()
            .0
            .into_iter()
            .zip_eq(schema)
            .map(|(datum, f)| (f.name.clone(), datum_to_json(&f.data_type, datum)))
            .collect();
        (row, Value::Object(record).to_string())
    }
}

fn datum_to_json(dt: &DataType, d: Datum) -> Value {
    match d {
        Some(v) => scalar_to_json(dt, v),
        None => Value::Null,
    }
}

fn struct_to_json(st: &StructType, s: StructValue) -> Map<String, Value> {
    st.field_names
        .iter()
        .zip_eq(&st.fields)
        .zip_eq(s.fields())
        .map(|((name, dt), value)| (name.clone(), datum_to_json(dt, value.clone())))
        .collect()
}

fn scalar_to_json(dt: &DataType, v: ScalarImpl) -> Value {
    if !SourceFormat::Json.supported_type(dt) {
        return Value::Null;
    }
    match v {
        ScalarImpl::Int16(i) => Value::Number(i.into()),
        ScalarImpl::Int32(i) => Value::Number(i.into()),
        ScalarImpl::Int64(i) => Value::Number(i.into()),
        ScalarImpl::Float32(i) => Value::Number(Number::from_f64(f64::from(i.0)).unwrap()),
        ScalarImpl::Float64(i) => Value::Number(Number::from_f64(i.0).unwrap()),
        ScalarImpl::Utf8(s) => Value::String(s),
        ScalarImpl::Bool(b) => Value::Bool(b),
        ScalarImpl::Decimal(d) => Value::Number(Number::from_f64(d.to_f64().unwrap()).unwrap()),
        ScalarImpl::Interval(_) => unreachable!(),
        ScalarImpl::NaiveDate(t) => Value::String(t.to_string()),
        ScalarImpl::NaiveDateTime(t) => Value::String(t.to_string()),
        ScalarImpl::NaiveTime(t) => Value::String(t.to_string()),
        ScalarImpl::Struct(s) => {
            let st = try_match_expand!(dt, DataType::Struct).unwrap();
            Value::Object(struct_to_json(st, s))
        }
        ScalarImpl::List(_) => unreachable!(),
    }
}
