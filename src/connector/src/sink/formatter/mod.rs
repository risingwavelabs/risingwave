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

use risingwave_common::array::Op;
use risingwave_common::row::Row;

use crate::sink::Result;

mod append_only;
mod upsert;

pub use append_only::AppendOnlyFormatter;
pub use upsert::UpsertFormatter;

pub trait SinkFormatter {
    type K;
    type V;
    type I: IntoIterator<Item = (Option<Self::K>, Option<Self::V>)>;

    fn format_row(&mut self, op: Op, row: impl Row + Copy) -> Result<Self::I>;
}
