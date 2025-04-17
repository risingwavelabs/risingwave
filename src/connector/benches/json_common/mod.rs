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

//! Common utilities shared by JSON parser benchmarks.

use rand::distr::Alphanumeric;
use rand::prelude::*;
use risingwave_common::catalog::ColumnId;
use risingwave_common::types::{DataType, Date, Timestamp};
use risingwave_connector::source::SourceColumnDesc;

pub const NUM_RECORDS: usize = 1 << 18; // ~ 250,000

pub fn generate_json_row(rng: &mut impl Rng) -> String {
    format!(
        "{{\"i32\":{},\"bool\":{},\"i16\":{},\"i64\":{},\"f32\":{},\"f64\":{},\"varchar\":\"{}\",\"date\":\"{}\",\"timestamp\":\"{}\"}}",
        rng.random::<i32>(),
        rng.random::<bool>(),
        rng.random::<i16>(),
        rng.random::<i64>(),
        rng.random::<f32>(),
        rng.random::<f64>(),
        rng.sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect::<String>(),
        Date::from_num_days_from_ce_uncheck((rng.random::<u32>() % (1 << 20)) as i32).0,
        {
            let datetime =
                Timestamp::from_timestamp_uncheck((rng.random::<u32>() % (1u32 << 28)) as i64, 0).0;
            format!("{:?} {:?}", datetime.date(), datetime.time())
        }
    )
}

pub fn get_descs() -> Vec<SourceColumnDesc> {
    vec![
        SourceColumnDesc::simple("i32", DataType::Int32, ColumnId::from(0)),
        SourceColumnDesc::simple("bool", DataType::Boolean, ColumnId::from(2)),
        SourceColumnDesc::simple("i16", DataType::Int16, ColumnId::from(3)),
        SourceColumnDesc::simple("i64", DataType::Int64, ColumnId::from(4)),
        SourceColumnDesc::simple("f32", DataType::Float32, ColumnId::from(5)),
        SourceColumnDesc::simple("f64", DataType::Float64, ColumnId::from(6)),
        SourceColumnDesc::simple("varchar", DataType::Varchar, ColumnId::from(7)),
        SourceColumnDesc::simple("date", DataType::Date, ColumnId::from(8)),
        SourceColumnDesc::simple("timestamp", DataType::Timestamp, ColumnId::from(9)),
    ]
}
