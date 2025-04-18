// Copyright 2025 RisingWave Labs
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

// These mods imports arrow_impl.rs to provide FromArrow, ToArrow traits for corresponding arrow versions,
// and the default From/To implementations.

mod arrow_52;
mod arrow_53;
mod arrow_54;
// These mods import mods above and may override some methods.
mod arrow_deltalake;
mod arrow_iceberg;
mod arrow_udf;

pub use arrow_deltalake::DeltaLakeConvert;
pub use arrow_iceberg::{
    ICEBERG_DECIMAL_PRECISION, ICEBERG_DECIMAL_SCALE, IcebergArrowConvert,
    IcebergCreateTableArrowConvert,
};
pub use arrow_udf::UdfArrowConvert;
pub use reexport::*;
/// For other RisingWave crates, they can directly use arrow re-exported here, without adding
/// `arrow` dependencies in their `Cargo.toml`. And they don't need to care about the version.
mod reexport {
    pub use super::arrow_deltalake::{
        FromArrow as DeltaLakeFromArrow, ToArrow as DeltaLakeToArrow,
        arrow_array as arrow_array_deltalake, arrow_buffer as arrow_buffer_deltalake,
        arrow_cast as arrow_cast_deltalake, arrow_schema as arrow_schema_deltalake,
    };
    pub use super::arrow_iceberg::{
        FromArrow as IcebergFromArrow, ToArrow as IcebergToArrow,
        arrow_array as arrow_array_iceberg, arrow_buffer as arrow_buffer_iceberg,
        arrow_cast as arrow_cast_iceberg, arrow_schema as arrow_schema_iceberg,
        is_parquet_schema_match_source_schema,
    };
    pub use super::arrow_udf::{
        FromArrow as UdfFromArrow, ToArrow as UdfToArrow, arrow_array as arrow_array_udf,
        arrow_buffer as arrow_buffer_udf, arrow_cast as arrow_cast_udf,
        arrow_schema as arrow_schema_udf,
    };
}
use crate::types::Interval;

/// Arrow 52 changed the interval type from `i128` to `arrow_buffer::IntervalMonthDayNano`, so
/// we introduced this trait to customize the conversion in `arrow_impl.rs`.
/// We may delete this after all arrow versions are upgraded.
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
