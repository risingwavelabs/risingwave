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

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;

use super::AggKind;
use crate::expr::ExpressionRef;
use crate::function::args::FuncArgs;

/// Represents an aggregation function.
#[derive(Clone, Debug)]
pub struct AggCall {
    /// Aggregation kind for constructing agg state.
    pub kind: AggKind,
    /// Arguments of aggregation function input.
    pub args: FuncArgs,
    /// The return type of aggregation function.
    pub return_type: DataType,

    /// Order requirements specified in order by clause of agg call
    pub column_orders: Vec<ColumnOrder>,

    /// Whether the stream is append-only.
    /// Specific streaming aggregator may optimize its implementation
    /// based on this knowledge.
    pub append_only: bool,

    /// Filter of aggregation.
    pub filter: Option<ExpressionRef>,

    /// Should deduplicate the input before aggregation.
    pub distinct: bool,
}
