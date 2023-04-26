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

//! Aggregation function definitions.

use std::sync::Arc;

use parse_display::{Display, FromStr};
use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::expr::agg_call::PbType;
use risingwave_pb::expr::PbAggCall;

use crate::expr::{build_from_prost, ExpressionRef};
use crate::Result;

/// Represents an aggregation function.
#[derive(Clone, Debug)]
pub struct AggCall {
    /// Aggregation kind for constructing agg state.
    pub kind: AggKind,
    /// Arguments of aggregation function input.
    pub args: AggArgs,
    /// The return type of aggregation function.
    pub return_type: DataType,

    /// Order requirements specified in order by clause of agg call
    pub column_orders: Vec<ColumnOrder>,

    /// Filter of aggregation.
    pub filter: Option<ExpressionRef>,

    /// Should deduplicate the input before aggregation.
    pub distinct: bool,
}

impl AggCall {
    pub fn from_protobuf(agg_call: &PbAggCall) -> Result<Self> {
        let agg_kind = AggKind::from_protobuf(agg_call.get_type()?)?;
        let args = match &agg_call.get_args()[..] {
            [] => AggArgs::None,
            [arg] => AggArgs::Unary(DataType::from(arg.get_type()?), arg.get_index() as usize),
            [agg_arg, extra_arg] => AggArgs::Binary(
                [
                    DataType::from(agg_arg.get_type()?),
                    DataType::from(extra_arg.get_type()?),
                ],
                [agg_arg.get_index() as usize, extra_arg.get_index() as usize],
            ),
            _ => bail!("Too many arguments for {:?}", agg_kind),
        };
        let column_orders = agg_call
            .get_order_by()
            .iter()
            .map(|col_order| {
                let col_idx = col_order.get_column_index() as usize;
                let order_type = OrderType::from_protobuf(col_order.get_order_type().unwrap());
                ColumnOrder::new(col_idx, order_type)
            })
            .collect();
        let filter = match agg_call.filter {
            Some(ref pb_filter) => Some(Arc::from(build_from_prost(pb_filter)?)),
            None => None,
        };
        Ok(AggCall {
            kind: agg_kind,
            args,
            return_type: DataType::from(agg_call.get_return_type()?),
            column_orders,
            filter,
            distinct: agg_call.distinct,
        })
    }
}

/// Kind of aggregation function
#[derive(Debug, Display, FromStr, Copy, Clone, PartialEq, Eq, Hash)]
#[display(style = "snake_case")]
pub enum AggKind {
    BitAnd,
    BitOr,
    BitXor,
    BoolAnd,
    BoolOr,
    Min,
    Max,
    Sum,
    Sum0,
    Count,
    Avg,
    StringAgg,
    ApproxCountDistinct,
    ArrayAgg,
    JsonbAgg,
    JsonbObjectAgg,
    FirstValue,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
}

impl AggKind {
    pub fn from_protobuf(pb_type: PbType) -> Result<Self> {
        match pb_type {
            PbType::BitAnd => Ok(AggKind::BitAnd),
            PbType::BitOr => Ok(AggKind::BitOr),
            PbType::BitXor => Ok(AggKind::BitXor),
            PbType::BoolAnd => Ok(AggKind::BoolAnd),
            PbType::BoolOr => Ok(AggKind::BoolOr),
            PbType::Min => Ok(AggKind::Min),
            PbType::Max => Ok(AggKind::Max),
            PbType::Sum => Ok(AggKind::Sum),
            PbType::Sum0 => Ok(AggKind::Sum0),
            PbType::Avg => Ok(AggKind::Avg),
            PbType::Count => Ok(AggKind::Count),
            PbType::StringAgg => Ok(AggKind::StringAgg),
            PbType::ApproxCountDistinct => Ok(AggKind::ApproxCountDistinct),
            PbType::ArrayAgg => Ok(AggKind::ArrayAgg),
            PbType::JsonbAgg => Ok(AggKind::JsonbAgg),
            PbType::JsonbObjectAgg => Ok(AggKind::JsonbObjectAgg),
            PbType::FirstValue => Ok(AggKind::FirstValue),
            PbType::StddevPop => Ok(AggKind::StddevPop),
            PbType::StddevSamp => Ok(AggKind::StddevSamp),
            PbType::VarPop => Ok(AggKind::VarPop),
            PbType::VarSamp => Ok(AggKind::VarSamp),
            PbType::Unspecified => bail!("Unrecognized agg."),
        }
    }

    pub fn to_protobuf(self) -> PbType {
        match self {
            Self::BitAnd => PbType::BitAnd,
            Self::BitOr => PbType::BitOr,
            Self::BitXor => PbType::BitXor,
            Self::BoolAnd => PbType::BoolAnd,
            Self::BoolOr => PbType::BoolOr,
            Self::Min => PbType::Min,
            Self::Max => PbType::Max,
            Self::Sum => PbType::Sum,
            Self::Sum0 => PbType::Sum0,
            Self::Avg => PbType::Avg,
            Self::Count => PbType::Count,
            Self::StringAgg => PbType::StringAgg,
            Self::ApproxCountDistinct => PbType::ApproxCountDistinct,
            Self::ArrayAgg => PbType::ArrayAgg,
            Self::JsonbAgg => PbType::JsonbAgg,
            Self::JsonbObjectAgg => PbType::JsonbObjectAgg,
            Self::FirstValue => PbType::FirstValue,
            Self::StddevPop => PbType::StddevPop,
            Self::StddevSamp => PbType::StddevSamp,
            Self::VarPop => PbType::VarPop,
            Self::VarSamp => PbType::VarSamp,
        }
    }
}

/// An aggregation function may accept 0, 1 or 2 arguments.
#[derive(Clone, Debug)]
pub enum AggArgs {
    /// `None` is used for function calls that accept 0 argument, e.g. `count(*)`.
    None,
    /// `Unary` is used for function calls that accept 1 argument, e.g. `sum(x)`.
    Unary(DataType, usize),
    /// `Binary` is used for function calls that accept 2 arguments, e.g. `string_agg(x, delim)`.
    Binary([DataType; 2], [usize; 2]),
}

impl AggArgs {
    /// return the types of arguments.
    pub fn arg_types(&self) -> &[DataType] {
        use AggArgs::*;
        match self {
            None => &[],
            Unary(typ, _) => std::slice::from_ref(typ),
            Binary(typs, _) => typs,
        }
    }

    /// return the indices of the arguments in [`risingwave_common::array::StreamChunk`].
    pub fn val_indices(&self) -> &[usize] {
        use AggArgs::*;
        match self {
            None => &[],
            Unary(_, val_idx) => std::slice::from_ref(val_idx),
            Binary(_, val_indices) => val_indices,
        }
    }
}
