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

mod avro;
mod json;
mod protobuf;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
pub use json::JsonGen;
use rand::distributions::Alphanumeric;
use rand::Rng;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::decimal::FromPrimitive;
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::{
    DataType, DataTypeName, Datum, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper,
    NaiveTimeWrapper, ScalarImpl,
};
use risingwave_source::{SourceColumnDesc, SourceFormat};
use strum::IntoEnumIterator;

fn default_data_type(tname: &DataTypeName) -> DataType {
    tname.to_type().unwrap_or_else(|| match tname {
        DataTypeName::Struct => DataType::Struct(Arc::new(StructType::new(vec![]))),
        DataTypeName::List => DataType::List {
            datatype: Box::new(DataType::Boolean),
        },
        _ => unreachable!(),
    })
}

pub fn create_relational_schema(source_format: SourceFormat) -> Vec<SourceColumnDesc> {
    let mut i = 0;
    DataTypeName::iter()
        .filter(|tname| source_format.supported_type(&default_data_type(tname)))
        .map(|tname: DataTypeName| {
            i += 1;

            SourceColumnDesc {
                name: format!("{}", i),
                data_type: default_data_type(&tname),
                column_id: i.into(),
                fields: vec![],
                skip_parse: false,
            }
        })
        .collect()
}

// TODO: This function has many duplicates with `field_generator`.
// Maybe we can merge them into the same function.
fn gen_datum<R: Rng>(rng: &mut R, dt: &DataType) -> Datum {
    use DataType as T;
    if rng.gen_range(1..=10) == 1 {
        // 10% chance to generate NULL.
        return None;
    }
    let v = match dt {
        T::Boolean => rng.gen_bool(0.5).into(),
        T::Int16 => rng.gen_range(i16::MIN..=i16::MAX).into(),
        T::Int32 => rng.gen_range(i32::MIN..=i32::MAX).into(),
        T::Int64 => rng.gen_range(i64::MIN..=i64::MAX).into(),
        T::Float32 => (rng.gen_range(-1_000_000.0..=1_000_000.0) as f32).into(),
        T::Float64 => rng
            .gen_range(-1_000_000_000_000.0..=1_000_000_000_000.0)
            .into(),
        T::Decimal => ScalarImpl::Decimal(
            Decimal::from_f64(rng.gen_range(-1_000_000_000_000.0..=1_000_000_000_000.0)).unwrap(),
        ),
        T::Varchar => (0..10)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect::<String>()
            .into(),
        T::Date => NaiveDateWrapper(NaiveDate::from_ymd(
            rng.gen_range(1979..=2022),
            rng.gen_range(1..=12),
            rng.gen_range(1..=20),
        ))
        .into(),
        T::Time => NaiveTimeWrapper(NaiveTime::from_hms(
            rng.gen_range(0..=23),
            rng.gen_range(0..=59),
            rng.gen_range(0..=59),
        ))
        .into(),
        T::Timestamp | T::Timestampz => NaiveDateTimeWrapper(NaiveDateTime::from_timestamp(
            rng.gen_range(1000000000..=1662346644),
            0,
        ))
        .into(),
        T::Interval => IntervalUnit::from_minutes(rng.gen_range(0..=60 * 24 * 365)).into(),
        T::Struct(st) => {
            let fields = st
                .fields
                .iter()
                .map(|field_type| gen_datum(rng, field_type))
                .collect();
            StructValue::new(fields).into()
        }
        T::List { datatype } => {
            let list_len = rng.gen_range(0..4);
            let elements = (0..=list_len)
                .into_iter()
                .map(|_| gen_datum(rng, datatype))
                .collect();
            ListValue::new(elements).into()
        }
    };
    Some(v)
}
