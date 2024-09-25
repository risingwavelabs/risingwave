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

#[allow(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;
type ArrowIntervalType = arrow_buffer::IntervalMonthDayNano;
pub use arrow_impl::{FromArrow, ToArrow};
pub use {
    arrow_53_array as arrow_array, arrow_53_buffer as arrow_buffer, arrow_53_cast as arrow_cast,
    arrow_53_schema as arrow_schema,
};

use crate::array::Interval;

impl super::ArrowIntervalTypeTrait for ArrowIntervalType {
    fn to_interval(self) -> Interval {
        // XXX: the arrow-rs decoding is incorrect
        // let (months, days, ns) = arrow_array::types::IntervalMonthDayNanoType::to_parts(value);
        Interval::from_month_day_usec(self.months, self.days, self.nanoseconds / 1000)
    }

    fn from_interval(value: Interval) -> Self {
        // XXX: the arrow-rs encoding is incorrect
        // arrow_array::types::IntervalMonthDayNanoType::make_value(
        //     self.months(),
        //     self.days(),
        //     // TODO: this may overflow and we need `try_into`
        //     self.usecs() * 1000,
        // )
        Self {
            months: value.months(),
            days: value.days(),
            nanoseconds: value.usecs() * 1000,
        }
    }
}
