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

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_frontend::expr::DataTypeName;
use risingwave_sqlparser::ast::{DataType, Expr, Value};

use crate::expr::sql_null;
use crate::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_simple_scalar(&mut self, typ: DataTypeName) -> Expr {
        use DataTypeName as T;
        match typ {
            T::Int64 => Expr::Value(Value::Number(
                self.gen_int(i64::MIN as isize, i64::MAX as isize),
                false,
            )),
            T::Int32 => Expr::TypedString {
                data_type: DataType::Int(None),
                value: self.gen_int(i32::MIN as isize, i32::MAX as isize),
            },
            T::Int16 => Expr::TypedString {
                data_type: DataType::SmallInt(None),
                value: self.gen_int(i16::MIN as isize, i16::MAX as isize),
            },
            T::Varchar => Expr::Value(Value::SingleQuotedString(
                (0..10)
                    .map(|_| self.rng.sample(Alphanumeric) as char)
                    .collect(),
            )),
            T::Decimal => Expr::Value(Value::Number(self.gen_float(), false)),
            T::Float64 => Expr::TypedString {
                data_type: DataType::Float(None),
                value: self.gen_float(),
            },
            T::Float32 => Expr::TypedString {
                data_type: DataType::Real,
                value: self.gen_float(),
            },
            T::Boolean => Expr::Value(Value::Boolean(self.rng.gen_bool(0.5))),
            T::Date => Expr::TypedString {
                data_type: DataType::Date,
                value: self.gen_temporal_scalar(typ),
            },
            T::Time => Expr::TypedString {
                data_type: DataType::Time(false),
                value: self.gen_temporal_scalar(typ),
            },
            T::Timestamp | T::Timestampz => Expr::TypedString {
                data_type: DataType::Timestamp(false),
                value: self.gen_temporal_scalar(typ),
            },
            T::Interval => Expr::TypedString {
                data_type: DataType::Interval,
                value: self.gen_temporal_scalar(typ),
            },
            _ => sql_null(),
        }
    }

    fn gen_int(&mut self, _min: isize, max: isize) -> String {
        let n = match self.rng.gen_range(0..=4) {
            0 => 0,
            1 => 1,
            2 => max,
            // TODO: Negative numbers have a few issues.
            // - Parsing, tracked by: <https://github.com/risingwavelabs/risingwave/issues/4344>.
            // - Neg op with Interval, tracked by: <https://github.com/risingwavelabs/risingwave/issues/112>
            // 3 => i32::MIN as f64,
            3..=4 => self.rng.gen_range(1..max),
            _ => unreachable!(),
        };
        n.to_string()
    }

    fn gen_float(&mut self) -> String {
        let n = match self.rng.gen_range(0..=4) {
            0 => 0.0,
            1 => 1.0,
            2 => i32::MAX as f64,
            // TODO: Negative numbers have a few issues.
            // - Parsing, tracked by: <https://github.com/risingwavelabs/risingwave/issues/4344>.
            // - Neg op with Interval, tracked by: <https://github.com/risingwavelabs/risingwave/issues/112>
            // 3 => i32::MIN as f64,
            3..=4 => self.rng.gen_range(1.0..i32::MAX as f64),
            _ => unreachable!(),
        };
        n.to_string()
    }

    fn gen_temporal_scalar(&mut self, typ: DataTypeName) -> String {
        use DataTypeName as T;

        let rand_secs = self.rng.gen_range(2..1000000) as u64;
        let minute = 60;
        let hour = 60 * minute;
        let day = 24 * hour;
        let week = 7 * day;
        // `0` is not generated due to:
        // Tracking issue: <https://github.com/risingwavelabs/risingwave/issues/4504>
        // It is tracked under refinements:
        // <https://github.com/risingwavelabs/risingwave/issues/3896>
        let choices = [1, minute, hour, day, week, rand_secs];
        let secs = choices.choose(&mut self.rng).unwrap();

        let tm = DateTime::<Utc>::from(SystemTime::now() - Duration::from_secs(*secs));
        match typ {
            T::Date => tm.format("%F").to_string(),
            T::Timestamp | T::Timestampz => tm.format("%Y-%m-%d %H:%M:%S").to_string(),
            T::Time => tm.format("%T").to_string(),
            T::Interval => secs.to_string(),
            _ => unreachable!(),
        }
    }
}
