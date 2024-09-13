// Copyright 2024 RisingWave Labs
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

mod arrow_deltalake;
mod arrow_iceberg;
mod arrow_udf;

pub use arrow_deltalake::DeltaLakeConvert;
pub use arrow_iceberg::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
pub use arrow_udf::{FromArrow, ToArrow, UdfArrowConvert};

use crate::types::Interval;

trait ArrowIntervalTypeTrait {
    fn to_interval(self) -> Interval;
    fn from_interval(value: Interval) -> Self;
}

impl ArrowIntervalTypeTrait for i128 {
    fn to_interval(self) -> Interval {
        // XXX: the arrow-rs decoding is incorrect
        // let (months, days, ns) = arrow_array::types::IntervalMonthDayNanoType::to_parts(value);
        let months = self as i32;
        let days = (self >> 32) as i32;
        let ns = (self >> 64) as i64;
        Interval::from_month_day_usec(months, days, ns / 1000)
    }

    fn from_interval(value: Interval) -> i128 {
        // XXX: the arrow-rs encoding is incorrect
        // arrow_array::types::IntervalMonthDayNanoType::make_value(
        //     self.months(),
        //     self.days(),
        //     // TODO: this may overflow and we need `try_into`
        //     self.usecs() * 1000,
        // )
        let m = value.months() as u128 & u32::MAX as u128;
        let d = (value.days() as u128 & u32::MAX as u128) << 32;
        let n = ((value.usecs() * 1000) as u128 & u64::MAX as u128) << 64;
        (m | d | n) as i128
    }
}
