// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use apache_avro::schema::Schema as AvroSchema;
use apache_avro::types::{Record, Value};
use apache_avro::Writer;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, ScalarRefImpl, StructType};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};

use super::{FieldEncodeError, Result as SinkResult, RowEncoder, SerTo};

type Result<T> = std::result::Result<T, FieldEncodeError>;

pub struct AvroEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    avro_schema: Arc<AvroSchema>,
}

impl AvroEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        avro_schema: Arc<AvroSchema>,
    ) -> SinkResult<Self> {
        match &col_indices {
            Some(col_indices) => validate_fields(
                col_indices.iter().map(|idx| {
                    let f = &schema[*idx];
                    (f.name.as_str(), &f.data_type)
                }),
                &avro_schema,
            )?,
            None => validate_fields(
                schema
                    .fields
                    .iter()
                    .map(|f| (f.name.as_str(), &f.data_type)),
                &avro_schema,
            )?,
        };

        Ok(Self {
            schema,
            col_indices,
            avro_schema,
        })
    }
}

impl RowEncoder for AvroEncoder {
    type Output = (Value, Arc<AvroSchema>);

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_deref()
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> SinkResult<Self::Output> {
        let record = encode_fields(
            col_indices.map(|idx| {
                let f = &self.schema[idx];
                ((f.name.as_str(), &f.data_type), row.datum_at(idx))
            }),
            &self.avro_schema,
        )?;
        Ok((record.into(), self.avro_schema.clone()))
    }
}

impl SerTo<Vec<u8>> for (Value, Arc<AvroSchema>) {
    fn ser_to(self) -> SinkResult<Vec<u8>> {
        let mut w = Writer::new(&self.1, Vec::new());
        w.append(self.0)
            .and_then(|_| w.into_inner())
            .map_err(|e| crate::sink::SinkError::Encode(e.to_string()))
    }
}

enum OptIdx {
    NotUnion,
    Single,
    NullLeft,
    NullRight,
}

/// A trait that assists code reuse between `validate` and `encode`.
/// * For `validate`, the inputs are (RisingWave type, ProtoBuf type).
/// * For `encode`, the inputs are (RisingWave type, RisingWave data, ProtoBuf type).
///
/// Thus we impl [`MaybeData`] for both [`()`] and [`DatumRef`].
trait MaybeData: std::fmt::Debug {
    type Out;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out>;

    /// Switch to `RecordSchema` after #12562
    fn on_struct(self, st: &StructType, avro: &AvroSchema) -> Result<Self::Out>;

    fn on_list(self, elem: &DataType, avro: &AvroSchema) -> Result<Self::Out>;

    fn handle_union(out: Self::Out, opt_idx: OptIdx) -> Result<Self::Out>;
}

impl MaybeData for () {
    type Out = ();

    fn on_base(self, _: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        Ok(self)
    }

    fn on_struct(self, st: &StructType, avro: &AvroSchema) -> Result<Self::Out> {
        validate_fields(st.iter(), avro)
    }

    fn on_list(self, elem: &DataType, avro: &AvroSchema) -> Result<Self::Out> {
        encode_field(elem, (), avro)
    }

    fn handle_union(out: Self::Out, _: OptIdx) -> Result<Self::Out> {
        Ok(out)
    }
}

impl MaybeData for DatumRef<'_> {
    type Out = Value;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        match self {
            Some(s) => f(s),
            None => Ok(Value::Null),
        }
    }

    fn on_struct(self, st: &StructType, avro: &AvroSchema) -> Result<Self::Out> {
        let d = match self {
            Some(s) => s.into_struct(),
            None => return Ok(Value::Null),
        };
        let record = encode_fields(st.iter().zip_eq_debug(d.iter_fields_ref()), avro)?;
        Ok(record.into())
    }

    fn on_list(self, elem: &DataType, avro: &AvroSchema) -> Result<Self::Out> {
        let d = match self {
            Some(s) => s.into_list(),
            None => return Ok(Value::Null),
        };
        let vs = d
            .iter()
            .map(|d| encode_field(elem, d, avro))
            .try_collect()?;
        Ok(Value::Array(vs))
    }

    fn handle_union(out: Self::Out, opt_idx: OptIdx) -> Result<Self::Out> {
        use OptIdx::*;

        match out == Value::Null {
            true => {
                let ni = match opt_idx {
                    NotUnion | Single => {
                        return Err(FieldEncodeError::new("found null but required"))
                    }
                    NullLeft => 0,
                    NullRight => 1,
                };
                Ok(Value::Union(ni, out.into()))
            }
            false => {
                let vi = match opt_idx {
                    NotUnion => return Ok(out),
                    NullLeft => 1,
                    Single | NullRight => 0,
                };
                Ok(Value::Union(vi, out.into()))
            }
        }
    }
}

fn validate_fields<'rw>(
    rw_fields: impl Iterator<Item = (&'rw str, &'rw DataType)>,
    avro: &AvroSchema,
) -> Result<()> {
    let AvroSchema::Record { fields, lookup, .. } = avro else {
        return Err(FieldEncodeError::new(format!(
            "expect avro record but got {}",
            avro.canonical_form(),
        )));
    };
    let mut present = vec![false; fields.len()];
    for (name, t) in rw_fields {
        let Some(&idx) = lookup.get(name) else {
            return Err(FieldEncodeError::new("field not in avro").with_name(name));
        };
        present[idx] = true;
        let avro_field = &fields[idx];
        encode_field(t, (), &avro_field.schema).map_err(|e| e.with_name(name))?;
    }
    for (p, avro_field) in present.into_iter().zip_eq_fast(fields) {
        if p {
            continue;
        }
        if !avro_field.is_nullable() {
            return Err(
                FieldEncodeError::new("field not present but required").with_name(&avro_field.name)
            );
        }
    }
    Ok(())
}

fn encode_fields<'avro, 'rw>(
    fields_with_datums: impl Iterator<Item = ((&'rw str, &'rw DataType), DatumRef<'rw>)>,
    schema: &'avro AvroSchema,
) -> Result<Record<'avro>> {
    let mut record = Record::new(schema).unwrap();
    let AvroSchema::Record { fields, lookup, .. } = schema else {
        unreachable!()
    };
    let mut present = vec![false; fields.len()];
    for ((name, t), d) in fields_with_datums {
        let idx = lookup[name];
        present[idx] = true;
        let avro_field = &fields[idx];
        let value = encode_field(t, d, &avro_field.schema).map_err(|e| e.with_name(name))?;
        record.put(name, value);
    }
    // Unfortunately, the upstream `apache_avro` does not handle missing fields as nullable correctly.
    // The correct encoding is `Value::Union(null_index, Value::Null)` but it simply writes `Value::Null`.
    // See [`tests::test_encode_avro_lib_bug`].
    for (p, avro_field) in present.into_iter().zip_eq_fast(fields) {
        if p {
            continue;
        }
        let AvroSchema::Union(u) = &avro_field.schema else {
            unreachable!()
        };
        // We could have saved null index of each field during [`validate_fields`] to avoid repeated lookup.
        // But in most cases it is the 0th.
        // Alternatively, we can simplify by enforcing the best practice of `null at 0th`.
        let ni = u
            .variants()
            .iter()
            .position(|a| a == &AvroSchema::Null)
            .unwrap();
        record.put(
            &avro_field.name,
            Value::Union(ni.try_into().unwrap(), Value::Null.into()),
        );
    }
    Ok(record)
}

/// Handles both `validate` (without actual data) and `encode`.
/// See [`MaybeData`] for more info.
fn encode_field<D: MaybeData>(
    data_type: &DataType,
    maybe: D,
    expected: &AvroSchema,
) -> Result<D::Out> {
    use risingwave_common::types::Interval;

    let no_match_err = || {
        Err(FieldEncodeError::new(format!(
            "cannot encode {} column as {} field",
            data_type,
            expected.canonical_form()
        )))
    };

    if let AvroSchema::Ref { .. } = expected {
        return Err(FieldEncodeError::new("avro name ref unsupported yet"));
    }

    // For now, we only support optional single type, rather than general union.
    // For example, how do we encode int16 into avro `["int", "long"]`?
    let (inner, opt_idx) = match expected {
        AvroSchema::Union(union) => match union.variants() {
            [] => return no_match_err(),
            [one] => (one, OptIdx::Single),
            [AvroSchema::Null, r] => (r, OptIdx::NullLeft),
            [l, AvroSchema::Null] => (l, OptIdx::NullRight),
            _ => return no_match_err(),
        },
        _ => (expected, OptIdx::NotUnion),
    };

    let value = match &data_type {
        // Group A: perfect match between RisingWave types and Avro types
        DataType::Boolean => match inner {
            AvroSchema::Boolean => maybe.on_base(|s| Ok(Value::Boolean(s.into_bool())))?,
            _ => return no_match_err(),
        },
        DataType::Varchar => match inner {
            AvroSchema::String => maybe.on_base(|s| Ok(Value::String(s.into_utf8().into())))?,
            _ => return no_match_err(),
        },
        DataType::Bytea => match inner {
            AvroSchema::Bytes => maybe.on_base(|s| Ok(Value::Bytes(s.into_bytea().into())))?,
            _ => return no_match_err(),
        },
        DataType::Float32 => match inner {
            AvroSchema::Float => maybe.on_base(|s| Ok(Value::Float(s.into_float32().into())))?,
            _ => return no_match_err(),
        },
        DataType::Float64 => match inner {
            AvroSchema::Double => maybe.on_base(|s| Ok(Value::Double(s.into_float64().into())))?,
            _ => return no_match_err(),
        },
        DataType::Int32 => match inner {
            AvroSchema::Int => maybe.on_base(|s| Ok(Value::Int(s.into_int32())))?,
            _ => return no_match_err(),
        },
        DataType::Int64 => match inner {
            AvroSchema::Long => maybe.on_base(|s| Ok(Value::Long(s.into_int64())))?,
            _ => return no_match_err(),
        },
        DataType::Struct(st) => match inner {
            AvroSchema::Record { .. } => maybe.on_struct(st, inner)?,
            _ => return no_match_err(),
        },
        DataType::List(elem) => match inner {
            AvroSchema::Array(avro_elem) => maybe.on_list(elem, avro_elem)?,
            _ => return no_match_err(),
        },
        // Group B: match between RisingWave types and Avro logical types
        DataType::Timestamptz => match inner {
            AvroSchema::TimestampMicros => maybe.on_base(|s| {
                Ok(Value::TimestampMicros(
                    s.into_timestamptz().timestamp_micros(),
                ))
            })?,
            AvroSchema::TimestampMillis => maybe.on_base(|s| {
                Ok(Value::TimestampMillis(
                    s.into_timestamptz().timestamp_millis(),
                ))
            })?,
            _ => return no_match_err(),
        },
        DataType::Timestamp => return no_match_err(),
        DataType::Date => match inner {
            AvroSchema::Date => {
                maybe.on_base(|s| Ok(Value::Date(s.into_date().get_nums_days_unix_epoch())))?
            }
            _ => return no_match_err(),
        },
        DataType::Time => match inner {
            AvroSchema::TimeMicros => {
                maybe.on_base(|s| Ok(Value::TimeMicros(Interval::from(s.into_time()).usecs())))?
            }
            AvroSchema::TimeMillis => maybe.on_base(|s| {
                Ok(Value::TimeMillis(
                    (Interval::from(s.into_time()).usecs() / 1000)
                        .try_into()
                        .unwrap(),
                ))
            })?,
            _ => return no_match_err(),
        },
        DataType::Interval => match inner {
            AvroSchema::Duration => maybe.on_base(|s| {
                use apache_avro::{Days, Duration, Millis, Months};
                let iv = s.into_interval();

                let overflow = |_| FieldEncodeError::new(format!("{iv} overflows avro duration"));

                Ok(Value::Duration(Duration::new(
                    Months::new(iv.months().try_into().map_err(overflow)?),
                    Days::new(iv.days().try_into().map_err(overflow)?),
                    Millis::new((iv.usecs() / 1000).try_into().map_err(overflow)?),
                )))
            })?,
            _ => return no_match_err(),
        },
        // Group C: experimental
        DataType::Int16 => return no_match_err(),
        DataType::Decimal => return no_match_err(),
        DataType::Jsonb => return no_match_err(),
        // Group D: unsupported
        DataType::Serial | DataType::Int256 => {
            return no_match_err();
        }
    };

    D::handle_union(value, opt_idx)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{
        Date, Datum, Interval, ListValue, ScalarImpl, StructValue, Time, Timestamptz, ToDatumRef,
    };

    use super::*;

    fn test_ok(t: &DataType, d: Datum, avro: &str, expected: Value) {
        let avro_schema = AvroSchema::parse_str(avro).unwrap();
        let actual = encode_field(t, d.to_datum_ref(), &avro_schema).unwrap();
        assert_eq!(actual, expected);
    }

    fn test_err<D: MaybeData>(t: &DataType, d: D, avro: &str, expected: &str)
    where
        D::Out: std::fmt::Debug,
    {
        let avro_schema = AvroSchema::parse_str(avro).unwrap();
        let err = encode_field(t, d, &avro_schema).unwrap_err();
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_encode_avro_ok() {
        test_ok(
            &DataType::Boolean,
            Some(ScalarImpl::Bool(false)),
            r#""boolean""#,
            Value::Boolean(false),
        );

        test_ok(
            &DataType::Varchar,
            Some(ScalarImpl::Utf8("RisingWave".into())),
            r#""string""#,
            Value::String("RisingWave".into()),
        );

        test_ok(
            &DataType::Bytea,
            Some(ScalarImpl::Bytea([0xbe, 0xef].into())),
            r#""bytes""#,
            Value::Bytes([0xbe, 0xef].into()),
        );

        test_ok(
            &DataType::Float32,
            Some(ScalarImpl::Float32(3.5f32.into())),
            r#""float""#,
            Value::Float(3.5f32),
        );

        test_ok(
            &DataType::Float64,
            Some(ScalarImpl::Float64(4.25f64.into())),
            r#""double""#,
            Value::Double(4.25f64),
        );

        test_ok(
            &DataType::Int32,
            Some(ScalarImpl::Int32(16)),
            r#""int""#,
            Value::Int(16),
        );

        test_ok(
            &DataType::Int64,
            Some(ScalarImpl::Int64(std::i64::MAX)),
            r#""long""#,
            Value::Long(i64::MAX),
        );

        let tstz = "2018-01-26T18:30:09.453Z".parse().unwrap();
        test_ok(
            &DataType::Timestamptz,
            Some(ScalarImpl::Timestamptz(tstz)),
            r#"{"type": "long", "logicalType": "timestamp-micros"}"#,
            Value::TimestampMicros(tstz.timestamp_micros()),
        );
        test_ok(
            &DataType::Timestamptz,
            Some(ScalarImpl::Timestamptz(tstz)),
            r#"{"type": "long", "logicalType": "timestamp-millis"}"#,
            Value::TimestampMillis(tstz.timestamp_millis()),
        );

        test_ok(
            &DataType::Date,
            Some(ScalarImpl::Date(Date::from_ymd_uncheck(1970, 1, 2))),
            r#"{"type": "int", "logicalType": "date"}"#,
            Value::Date(1),
        );

        let tm = Time::from_num_seconds_from_midnight_uncheck(1000, 0);
        test_ok(
            &DataType::Time,
            Some(ScalarImpl::Time(tm)),
            r#"{"type": "long", "logicalType": "time-micros"}"#,
            Value::TimeMicros(1000 * 1_000_000),
        );
        test_ok(
            &DataType::Time,
            Some(ScalarImpl::Time(tm)),
            r#"{"type": "int", "logicalType": "time-millis"}"#,
            Value::TimeMillis(1000 * 1000),
        );

        test_ok(
            &DataType::Interval,
            Some(ScalarImpl::Interval(Interval::from_month_day_usec(
                13, 2, 1000000,
            ))),
            // https://github.com/apache/avro/pull/2283
            // r#"{"type": "fixed", "name": "Duration", "size": 12, "logicalType": "duration"}"#,
            r#"{"type": {"type": "fixed", "name": "Duration", "size": 12}, "logicalType": "duration"}"#,
            Value::Duration(apache_avro::Duration::new(
                apache_avro::Months::new(13),
                apache_avro::Days::new(2),
                apache_avro::Millis::new(1000),
            )),
        )
    }

    #[test]
    fn test_encode_avro_err() {
        test_err(
            &DataType::Struct(StructType::new(vec![
                (
                    "p",
                    DataType::Struct(StructType::new(vec![
                        ("x", DataType::Int32),
                        ("y", DataType::Int32),
                    ])),
                ),
                (
                    "q",
                    DataType::Struct(StructType::new(vec![
                        ("x", DataType::Int32),
                        ("y", DataType::Int32),
                    ])),
                ),
            ])),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Struct(StructValue::new(vec![
                    Some(ScalarImpl::Int32(-2)),
                    Some(ScalarImpl::Int32(-1)),
                ]))),
                Some(ScalarImpl::Struct(StructValue::new(vec![
                    Some(ScalarImpl::Int32(2)),
                    Some(ScalarImpl::Int32(1)),
                ]))),
            ])))
            .to_datum_ref(),
            r#"{
                "type": "record",
                "name": "Segment",
                "fields": [
                    {
                        "name": "p",
                        "type": {
                            "type": "record",
                            "name": "Point",
                            "fields": [
                                {
                                    "name": "x",
                                    "type": "int"
                                },
                                {
                                    "name": "y",
                                    "type": "int"
                                }
                            ]
                        }
                    },
                    {
                        "name": "q",
                        "type": "Point"
                    }
                ]
            }"#,
            "encode q error: avro name ref unsupported yet",
        );

        test_err(
            &DataType::Interval,
            Some(ScalarRefImpl::Interval(Interval::from_month_day_usec(
                -1,
                -1,
                i64::MAX,
            ))),
            // https://github.com/apache/avro/pull/2283
            r#"{"type": {"type": "fixed", "name": "Duration", "size": 12}, "logicalType": "duration"}"#,
            "encode  error: -1 mons -1 days +2562047788:00:54.775807 overflows avro duration",
        );

        let avro_schema = AvroSchema::parse_str(
            r#"{"type": "record", "name": "Root", "fields": [
                {"name": "f0", "type": "int"}
            ]}"#,
        )
        .unwrap();
        let mut record = Record::new(&avro_schema).unwrap();
        record.put("f0", Value::String("2".into()));
        let res: SinkResult<Vec<u8>> = (Value::from(record), Arc::new(avro_schema)).ser_to();
        assert_eq!(res.unwrap_err().to_string(), "Encode error: Value does not match schema: Reason: Unsupported value-schema combination");
    }

    #[test]
    fn test_encode_avro_record() {
        let avro_schema = AvroSchema::parse_str(
            r#"{
                "type": "record",
                "name": "Root",
                "fields": [
                    {"name": "req", "type": "int"},
                    {"name": "opt", "type": ["null", "long"]}
                ]
            }"#,
        )
        .unwrap();
        let avro_schema = Arc::new(avro_schema);

        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "opt"),
            Field::with_name(DataType::Int32, "req"),
        ]);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int64(31)),
            Some(ScalarImpl::Int32(15)),
        ]);
        let encoder = AvroEncoder::new(schema, None, avro_schema.clone()).unwrap();
        let actual = encoder.encode(row).unwrap();
        assert_eq!(
            actual.0,
            Value::Record(vec![
                ("req".into(), Value::Int(15)),
                ("opt".into(), Value::Union(1, Value::Long(31).into())),
            ])
        );

        let schema = Schema::new(vec![Field::with_name(DataType::Int32, "req")]);
        let row = OwnedRow::new(vec![Some(ScalarImpl::Int32(15))]);
        let encoder = AvroEncoder::new(schema, None, avro_schema.clone()).unwrap();
        let actual = encoder.encode(row).unwrap();
        assert_eq!(
            actual.0,
            Value::Record(vec![
                ("req".into(), Value::Int(15)),
                ("opt".into(), Value::Union(0, Value::Null.into())),
            ])
        );

        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "opt")]);
        let Err(err) = AvroEncoder::new(schema, None, avro_schema.clone()) else {
            panic!()
        };
        assert_eq!(
            err.to_string(),
            "Encode error: encode req error: field not present but required"
        );

        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "opt"),
            Field::with_name(DataType::Int32, "req"),
            Field::with_name(DataType::Varchar, "extra"),
        ]);
        let Err(err) = AvroEncoder::new(schema, None, avro_schema.clone()) else {
            panic!()
        };
        assert_eq!(
            err.to_string(),
            "Encode error: encode extra error: field not in avro"
        );

        let avro_schema = AvroSchema::parse_str(r#"["null", "long"]"#).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "opt")]);
        let Err(err) = AvroEncoder::new(schema, None, avro_schema.into()) else {
            panic!()
        };
        assert_eq!(
            err.to_string(),
            r#"Encode error: encode  error: expect avro record but got ["null","long"]"#
        );

        test_err(
            &DataType::Struct(StructType::new(vec![("f0", DataType::Boolean)])),
            (),
            r#"{"type": "record", "name": "T", "fields": [{"name": "f0", "type": "int"}]}"#,
            "encode f0 error: cannot encode boolean column as \"int\" field",
        );
    }

    #[test]
    fn test_encode_avro_array() {
        let avro_schema = r#"{
            "type": "array",
            "items": "int"
        }"#;

        test_ok(
            &DataType::List(DataType::Int32.into()),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Int32(4)),
                Some(ScalarImpl::Int32(5)),
            ]))),
            avro_schema,
            Value::Array(vec![Value::Int(4), Value::Int(5)]),
        );

        test_err(
            &DataType::List(DataType::Int32.into()),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Int32(4)),
                None,
            ])))
            .to_datum_ref(),
            avro_schema,
            "encode  error: found null but required",
        );

        test_ok(
            &DataType::List(DataType::Int32.into()),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Int32(4)),
                None,
            ]))),
            r#"{
                "type": "array",
                "items": ["null", "int"]
            }"#,
            Value::Array(vec![
                Value::Union(1, Value::Int(4).into()),
                Value::Union(0, Value::Null.into()),
            ]),
        );

        test_ok(
            &DataType::List(DataType::List(DataType::Int32.into()).into()),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::List(ListValue::new(vec![
                    Some(ScalarImpl::Int32(26)),
                    Some(ScalarImpl::Int32(29)),
                ]))),
                Some(ScalarImpl::List(ListValue::new(vec![
                    Some(ScalarImpl::Int32(46)),
                    Some(ScalarImpl::Int32(49)),
                ]))),
            ]))),
            r#"{
                "type": "array",
                "items": {
                    "type": "array",
                    "items": "int"
                }
            }"#,
            Value::Array(vec![
                Value::Array(vec![Value::Int(26), Value::Int(29)]),
                Value::Array(vec![Value::Int(46), Value::Int(49)]),
            ]),
        );

        test_err(
            &DataType::List(DataType::Boolean.into()),
            (),
            r#"{"type": "array", "items": "int"}"#,
            "encode  error: cannot encode boolean column as \"int\" field",
        );
    }

    #[test]
    fn test_encode_avro_union() {
        let t = &DataType::Timestamptz;
        let datum = Some(ScalarImpl::Timestamptz(Timestamptz::from_micros(1500)));
        let opt_micros = r#"["null", {"type": "long", "logicalType": "timestamp-micros"}]"#;
        let opt_millis = r#"["null", {"type": "long", "logicalType": "timestamp-millis"}]"#;
        let both = r#"[{"type": "long", "logicalType": "timestamp-millis"}, {"type": "long", "logicalType": "timestamp-micros"}]"#;
        let empty = "[]";
        let one = r#"[{"type": "long", "logicalType": "timestamp-millis"}]"#;
        let right = r#"[{"type": "long", "logicalType": "timestamp-millis"}, "null"]"#;

        test_ok(
            t,
            datum.clone(),
            opt_micros,
            Value::Union(1, Value::TimestampMicros(1500).into()),
        );
        test_ok(t, None, opt_micros, Value::Union(0, Value::Null.into()));
        test_ok(
            t,
            datum.clone(),
            opt_millis,
            Value::Union(1, Value::TimestampMillis(1).into()),
        );
        test_ok(t, None, opt_millis, Value::Union(0, Value::Null.into()));

        test_err(
            t,
            datum.to_datum_ref(),
            both,
            r#"encode  error: cannot encode timestamp with time zone column as [{"type":"long","logicalType":"timestamp-millis"},{"type":"long","logicalType":"timestamp-micros"}] field"#,
        );

        test_err(
            t,
            datum.to_datum_ref(),
            empty,
            "encode  error: cannot encode timestamp with time zone column as [] field",
        );

        test_ok(
            t,
            datum.clone(),
            one,
            Value::Union(0, Value::TimestampMillis(1).into()),
        );
        test_err(t, None, one, "encode  error: found null but required");

        test_ok(
            t,
            datum.clone(),
            right,
            Value::Union(0, Value::TimestampMillis(1).into()),
        );
        test_ok(t, None, right, Value::Union(1, Value::Null.into()));
    }

    /// This just demonstrates bugs of the upstream [`apache_avro`], rather than our encoder.
    /// The encoder is not using these buggy calls and is already tested above.
    #[test]
    fn test_encode_avro_lib_bug() {
        use apache_avro::Reader;

        // a record with 2 optional int fields
        let avro_schema = AvroSchema::parse_str(
            r#"{
                "type": "record",
                "name": "Root",
                "fields": [
                    {
                        "name": "f0",
                        "type": ["null", "int"]
                    },
                    {
                        "name": "f1",
                        "type": ["null", "int"]
                    }
                ]
            }"#,
        )
        .unwrap();

        let mut writer = Writer::new(&avro_schema, Vec::new());
        let mut record = Record::new(writer.schema()).unwrap();
        // f0 omitted, f1 = Int(3)
        record.put("f1", Value::Int(3));
        writer.append(record).unwrap();
        let encoded = writer.into_inner().unwrap();
        // writing produced no error, but read fails
        let reader = Reader::new(encoded.as_slice()).unwrap();
        for value in reader {
            assert_eq!(
                value.unwrap_err().to_string(),
                "Union index 3 out of bounds: 2"
            );
        }

        let mut writer = Writer::new(&avro_schema, Vec::new());
        let mut record = Record::new(writer.schema()).unwrap();
        // f0 omitted, f1 = Union(1, Int(3))
        record.put("f1", Value::Union(1, Value::Int(3).into()));
        writer.append(record).unwrap();
        let encoded = writer.into_inner().unwrap();
        // writing produced no error, but read returns wrong value
        let reader = Reader::new(encoded.as_slice()).unwrap();
        for value in reader {
            assert_eq!(
                value.unwrap(),
                Value::Record(vec![
                    ("f0".into(), Value::Union(1, Value::Int(3).into())),
                    ("f1".into(), Value::Union(0, Value::Null.into())),
                ])
            );
        }
    }
}
