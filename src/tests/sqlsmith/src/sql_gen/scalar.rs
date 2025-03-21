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

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use rand::Rng;
use rand::distr::Alphanumeric;
use rand::prelude::IndexedRandom;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Array, DataType as AstDataType, Expr, Value};

use crate::sql_gen::SqlGenerator;
use crate::sql_gen::expr::typed_null;

impl<R: Rng> SqlGenerator<'_, R> {
    /// Generates integer scalar expression.
    /// Bound: [start, end).
    /// Type: `DataType`.
    pub(super) fn gen_range_scalar(
        &mut self,
        typ: &DataType,
        start: i64,
        end: i64,
    ) -> Option<Expr> {
        use DataType as T;
        let value = self.rng.random_range(start..end).to_string();
        match *typ {
            T::Int64 => Some(Expr::TypedString {
                data_type: AstDataType::BigInt,
                value,
            }),
            T::Int32 => Some(Expr::TypedString {
                data_type: AstDataType::Int,
                value,
            }),
            T::Int16 => Some(Expr::TypedString {
                data_type: AstDataType::SmallInt,
                value,
            }),
            _ => None,
        }
    }

    pub(super) fn gen_simple_scalar(&mut self, typ: &DataType) -> Expr {
        use DataType as T;
        // NOTE(kwannoel): Since this generates many invalid queries,
        // its probability should be set to low, e.g. 0.02.
        // ENABLE: https://github.com/risingwavelabs/risingwave/issues/7327
        if self.rng.random_bool(0.0) {
            // NOTE(kwannoel): We generate Cast with NULL to avoid generating lots of ambiguous
            // expressions. For instance agg calls such as `max(NULL)` may be generated,
            // and coerced to VARCHAR, where we require a `NULL::int` instead.
            return typed_null(typ);
        }
        // Scalars which may generate negative numbers are wrapped in
        // `Nested` to ambiguity while parsing.
        // e.g. -1 becomes -(1).
        // See: https://github.com/risingwavelabs/risingwave/issues/4344
        match *typ {
            T::Int64 => Expr::Nested(Box::new(Expr::TypedString {
                data_type: AstDataType::BigInt,
                value: self.gen_int(i64::MIN as _, i64::MAX as _),
            })),
            T::Int32 => Expr::Nested(Box::new(Expr::TypedString {
                data_type: AstDataType::Int,
                value: self.gen_int(i32::MIN as _, i32::MAX as _),
            })),
            T::Int16 => Expr::Nested(Box::new(Expr::TypedString {
                data_type: AstDataType::SmallInt,
                value: self.gen_int(i16::MIN as _, i16::MAX as _),
            })),
            T::Varchar => Expr::Cast {
                // since we are generating random scalar literal, we should cast it to avoid unknown type
                expr: Box::new(Expr::Value(Value::SingleQuotedString(
                    (0..10)
                        .map(|_| self.rng.sample(Alphanumeric) as char)
                        .collect(),
                ))),
                data_type: AstDataType::Varchar,
            },
            T::Decimal => Expr::Nested(Box::new(Expr::Value(Value::Number(self.gen_float())))),
            T::Float64 => Expr::Nested(Box::new(Expr::TypedString {
                data_type: AstDataType::Float(None),
                value: self.gen_float(),
            })),
            T::Float32 => Expr::Nested(Box::new(Expr::TypedString {
                data_type: AstDataType::Real,
                value: self.gen_float(),
            })),
            T::Boolean => Expr::Value(Value::Boolean(self.rng.random_bool(0.5))),
            T::Date => Expr::TypedString {
                data_type: AstDataType::Date,
                value: self.gen_temporal_scalar(typ),
            },
            T::Time => Expr::TypedString {
                data_type: AstDataType::Time(false),
                value: self.gen_temporal_scalar(typ),
            },
            T::Timestamp => Expr::TypedString {
                data_type: AstDataType::Timestamp(false),
                value: self.gen_temporal_scalar(typ),
            },
            T::Timestamptz => Expr::TypedString {
                data_type: AstDataType::Timestamp(true),
                value: self.gen_temporal_scalar(typ),
            },
            T::Interval => Expr::Nested(Box::new(Expr::TypedString {
                data_type: AstDataType::Interval,
                value: self.gen_temporal_scalar(typ),
            })),
            T::List(ref ty) => {
                let n = self.rng.random_range(1..=4);
                Expr::Array(Array {
                    elem: self.gen_simple_scalar_list(ty, n),
                    named: true,
                })
            }
            // ENABLE: https://github.com/risingwavelabs/risingwave/issues/6934
            // T::Struct(ref inner) => Expr::Row(
            //     inner
            //         .fields
            //         .iter()
            //         .map(|typ| self.gen_simple_scalar(typ))
            //         .collect(),
            // ),
            _ => typed_null(typ),
        }
    }

    /// Generates a list of `n` simple scalar values of a specific `type`.
    fn gen_simple_scalar_list(&mut self, ty: &DataType, n: usize) -> Vec<Expr> {
        (0..n).map(|_| self.gen_simple_scalar(ty)).collect()
    }

    fn gen_int(&mut self, min: i64, max: i64) -> String {
        // NOTE: Reduced chance for extreme values,
        // since these tend to generate invalid expressions.
        let n = match self.rng.random_range(1..=100) {
            1..=5 => 0,
            6..=10 => 1,
            11..=15 => max,
            16..=20 => min,
            21..=25 => self.rng.random_range(min + 1..0),
            26..=30 => self.rng.random_range(1000..max),
            31..=100 => self.rng.random_range(2..1000),
            _ => unreachable!(),
        };
        n.to_string()
    }

    fn gen_float(&mut self) -> String {
        // NOTE: Reduced chance for extreme values,
        // since these tend to generate invalid expressions.
        let n = match self.rng.random_range(1..=100) {
            1..=5 => 0.0,
            6..=10 => 1.0,
            11..=15 => i32::MAX as f64,
            16..=20 => i32::MIN as f64,
            21..=25 => self.rng.random_range(i32::MIN + 1..0) as f64,
            26..=30 => self.rng.random_range(1000..i32::MAX) as f64,
            31..=100 => self.rng.random_range(2..1000) as f64,
            _ => unreachable!(),
        };
        n.to_string()
    }

    fn gen_temporal_scalar(&mut self, typ: &DataType) -> String {
        use DataType as T;

        let minute = 60;
        let hour = 60 * minute;
        let day = 24 * hour;
        let week = 7 * day;
        let choices = [0, 1, minute, hour, day, week];

        let secs = match self.rng.random_range(1..=100) {
            1..=30 => *choices.choose(&mut self.rng).unwrap(),
            31..=100 => self.rng.random_range(2..100) as u64,
            _ => unreachable!(),
        };

        let tm = DateTime::<Utc>::from(SystemTime::now() - Duration::from_secs(secs));
        match typ {
            T::Date => tm.format("%F").to_string(),
            T::Timestamp | T::Timestamptz => tm.format("%Y-%m-%d %H:%M:%S").to_string(),
            // ENABLE: https://github.com/risingwavelabs/risingwave/issues/5826
            // T::Timestamptz => {
            //     let timestamp = tm.format("%Y-%m-%d %H:%M:%S");
            //     let timezone = self.rng.random_range(0..=15);
            //     format!("{}+{}", timestamp, timezone)
            // }
            T::Time => tm.format("%T").to_string(),
            T::Interval => {
                if self.rng.random_bool(0.5) {
                    (-(secs as i64)).to_string()
                } else {
                    secs.to_string()
                }
            }
            _ => unreachable!(),
        }
    }
}
