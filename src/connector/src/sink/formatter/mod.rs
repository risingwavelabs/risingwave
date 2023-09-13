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

use risingwave_common::array::{Op, RowRef};

use crate::sink::Result;

mod append_only;
mod debezium_json;
mod upsert;

pub use append_only::AppendOnlyFormatter;
#[cfg(test)]
pub(crate) use debezium_json::schema_to_json;
pub use debezium_json::{DebeziumAdapterOpts, DebeziumJsonFormatter};
pub use upsert::UpsertFormatter;

pub trait SinkFormatter {
    type K;
    type V;
    fn format_row(&mut self, op: Op, row: RowRef<'_>) -> Result<FormattedRow<Self::K, Self::V>>;
}

pub enum FormattedRow<K, V> {
    Skip,
    Pair(K, V),
    // Tomestone event
    // https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-delete-events
    WithTombstone(K, V),
}
