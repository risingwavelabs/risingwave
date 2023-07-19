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

use std::iter::Peekable;
use std::sync::Arc;

use itertools::Itertools;
use parse_display::{Display, FromStr};
use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding;
use risingwave_pb::expr::agg_call::PbType;
use risingwave_pb::expr::{PbAggCall, PbInputRef};

use crate::expr::{
    build_from_prost, BoxedExpression, ExpectExt, ExpressionRef, LiteralExpression, Token,
};
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

    /// Constant arguments.
    pub direct_args: Vec<LiteralExpression>,
}

impl AggCall {
    pub fn from_protobuf(agg_call: &PbAggCall) -> Result<Self> {
        let agg_kind = AggKind::from_protobuf(agg_call.get_type()?)?;
        let args = AggArgs::from_protobuf(agg_call.get_args())?;
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
        let direct_args = agg_call
            .direct_args
            .iter()
            .map(|arg| {
                let data_type = DataType::from(arg.get_type().unwrap());
                LiteralExpression::new(
                    data_type.clone(),
                    value_encoding::deserialize_datum(
                        arg.get_datum().unwrap().get_body().as_slice(),
                        &data_type,
                    )
                    .unwrap(),
                )
            })
            .collect_vec();
        Ok(AggCall {
            kind: agg_kind,
            args,
            return_type: DataType::from(agg_call.get_return_type()?),
            column_orders,
            filter,
            distinct: agg_call.distinct,
            direct_args,
        })
    }

    /// Build an `AggCall` from a string.
    ///
    /// # Syntax
    ///
    /// ```text
    /// (<name>:<type> [<index>:<type>]* [distinct] [orderby [<index>:<asc|desc>]*])
    /// ```
    pub fn from_pretty(s: impl AsRef<str>) -> Self {
        let tokens = crate::expr::lexer(s.as_ref());
        Parser::new(tokens.into_iter()).parse_aggregation()
    }

    pub fn with_filter(mut self, filter: BoxedExpression) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

struct Parser<Iter: Iterator> {
    tokens: Peekable<Iter>,
}

impl<Iter: Iterator<Item = Token>> Parser<Iter> {
    fn new(tokens: Iter) -> Self {
        Self {
            tokens: tokens.peekable(),
        }
    }

    fn parse_aggregation(&mut self) -> AggCall {
        assert_eq!(self.tokens.next(), Some(Token::LParen), "Expected a (");
        let func = self.parse_function();
        assert_eq!(self.tokens.next(), Some(Token::Colon), "Expected a Colon");
        let ty = self.parse_type();

        let mut distinct = false;
        let mut children = Vec::new();
        let mut column_orders = Vec::new();
        while matches!(self.tokens.peek(), Some(Token::Index(_))) {
            children.push(self.parse_arg());
        }
        if matches!(self.tokens.peek(), Some(Token::Literal(s)) if s == "distinct") {
            distinct = true;
            self.tokens.next(); // Consume
        }
        if matches!(self.tokens.peek(), Some(Token::Literal(s)) if s == "orderby") {
            self.tokens.next(); // Consume
            while matches!(self.tokens.peek(), Some(Token::Index(_))) {
                column_orders.push(self.parse_orderkey());
            }
        }
        self.tokens.next(); // Consume the RParen

        AggCall {
            kind: AggKind::from_protobuf(func).unwrap(),
            args: match children.as_slice() {
                [] => AggArgs::None,
                [(i, t)] => AggArgs::Unary(t.clone(), *i),
                [(i0, t0), (i1, t1)] => AggArgs::Binary([t0.clone(), t1.clone()], [*i0, *i1]),
                _ => panic!("too many arguments for agg call"),
            },
            return_type: ty,
            column_orders,
            filter: None,
            distinct,
            direct_args: Vec::new(),
        }
    }

    fn parse_type(&mut self) -> DataType {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(name) => name.parse::<DataType>().expect_str("type", &name),
            t => panic!("Expected a Literal, got {t:?}"),
        }
    }

    fn parse_arg(&mut self) -> (usize, DataType) {
        let idx = match self.tokens.next().expect("Unexpected end of input") {
            Token::Index(idx) => idx,
            t => panic!("Expected an Index, got {t:?}"),
        };
        assert_eq!(self.tokens.next(), Some(Token::Colon), "Expected a Colon");
        let ty = self.parse_type();
        (idx, ty)
    }

    fn parse_function(&mut self) -> PbType {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(name) => {
                PbType::from_str_name(&name.to_uppercase()).expect_str("function", &name)
            }
            t => panic!("Expected a Literal, got {t:?}"),
        }
    }

    fn parse_orderkey(&mut self) -> ColumnOrder {
        let idx = match self.tokens.next().expect("Unexpected end of input") {
            Token::Index(idx) => idx,
            t => panic!("Expected an Index, got {t:?}"),
        };
        assert_eq!(self.tokens.next(), Some(Token::Colon), "Expected a Colon");
        let order = match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(s) if s == "asc" => OrderType::ascending(),
            Token::Literal(s) if s == "desc" => OrderType::descending(),
            t => panic!("Expected asc or desc, got {t:?}"),
        };
        ColumnOrder::new(idx, order)
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
    LastValue,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    PercentileCont,
    PercentileDisc,
    Mode,
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
            PbType::LastValue => Ok(AggKind::LastValue),
            PbType::StddevPop => Ok(AggKind::StddevPop),
            PbType::StddevSamp => Ok(AggKind::StddevSamp),
            PbType::VarPop => Ok(AggKind::VarPop),
            PbType::VarSamp => Ok(AggKind::VarSamp),
            PbType::PercentileCont => Ok(AggKind::PercentileCont),
            PbType::PercentileDisc => Ok(AggKind::PercentileDisc),
            PbType::Mode => Ok(AggKind::Mode),
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
            Self::LastValue => PbType::LastValue,
            Self::StddevPop => PbType::StddevPop,
            Self::StddevSamp => PbType::StddevSamp,
            Self::VarPop => PbType::VarPop,
            Self::VarSamp => PbType::VarSamp,
            Self::PercentileCont => PbType::PercentileCont,
            Self::PercentileDisc => PbType::PercentileDisc,
            Self::Mode => PbType::Mode,
        }
    }
}

/// Macros to generate match arms for [`AggKind`].
/// IMPORTANT: These macros must be carefully maintained especially when adding new [`AggKind`]
/// variants.
pub mod agg_kinds {
    /// [`AggKind`]s that are currently not supported in streaming mode.
    #[macro_export]
    macro_rules! unimplemented_in_stream {
        () => {
            AggKind::BitAnd
                | AggKind::BitOr
                | AggKind::BoolAnd
                | AggKind::BoolOr
                | AggKind::JsonbAgg
                | AggKind::JsonbObjectAgg
                | AggKind::PercentileCont
                | AggKind::PercentileDisc
                | AggKind::Mode
        };
    }
    pub use unimplemented_in_stream;

    /// [`AggKind`]s that should've been rewritten to other kinds. These kinds should not appear
    /// when generating physical plan nodes.
    #[macro_export]
    macro_rules! rewritten {
        () => {
            AggKind::Avg
                | AggKind::StddevPop
                | AggKind::StddevSamp
                | AggKind::VarPop
                | AggKind::VarSamp
        };
    }
    pub use rewritten;

    /// [`AggKind`]s of which the aggregate results are not affected by the user given ORDER BY
    /// clause.
    #[macro_export]
    macro_rules! result_unaffected_by_order_by {
        () => {
            AggKind::BitAnd
                | AggKind::BitOr
                | AggKind::BitXor // XOR is commutative and associative
                | AggKind::BoolAnd
                | AggKind::BoolOr
                | AggKind::Min
                | AggKind::Max
                | AggKind::Sum
                | AggKind::Sum0
                | AggKind::Count
                | AggKind::Avg
                | AggKind::ApproxCountDistinct
                | AggKind::VarPop
                | AggKind::VarSamp
                | AggKind::StddevPop
                | AggKind::StddevSamp
        };
    }
    pub use result_unaffected_by_order_by;

    /// [`AggKind`]s that must be called with ORDER BY clause. These are slightly different from
    /// variants not in [`result_unaffected_by_order_by`], in that variants returned by this macro
    /// should be banned while the others should just be warned.
    #[macro_export]
    macro_rules! must_have_order_by {
        () => {
            AggKind::FirstValue
                | AggKind::LastValue
                | AggKind::PercentileCont
                | AggKind::PercentileDisc
                | AggKind::Mode
        };
    }
    pub use must_have_order_by;

    /// [`AggKind`]s of which the aggregate results are not affected by the user given DISTINCT
    /// keyword.
    #[macro_export]
    macro_rules! result_unaffected_by_distinct {
        () => {
            AggKind::BitAnd
                | AggKind::BitOr
                | AggKind::BoolAnd
                | AggKind::BoolOr
                | AggKind::Min
                | AggKind::Max
                | AggKind::ApproxCountDistinct
        };
    }
    pub use result_unaffected_by_distinct;

    /// [`AggKind`]s that are simply cannot 2-phased.
    #[macro_export]
    macro_rules! simply_cannot_two_phase {
        () => {
            AggKind::StringAgg
                | AggKind::ApproxCountDistinct
                | AggKind::ArrayAgg
                | AggKind::JsonbAgg
                | AggKind::JsonbObjectAgg
                | AggKind::FirstValue
                | AggKind::LastValue
                | AggKind::PercentileCont
                | AggKind::PercentileDisc
                | AggKind::Mode
        };
    }
    pub use simply_cannot_two_phase;

    /// [`AggKind`]s that are implemented with a single value state (so-called stateless).
    #[macro_export]
    macro_rules! single_value_state {
        () => {
            AggKind::Sum | AggKind::Sum0 | AggKind::Count | AggKind::BitXor
        };
    }
    pub use single_value_state;

    /// [`AggKind`]s that are implemented with a single value state (so-called stateless) iff the
    /// input is append-only.
    #[macro_export]
    macro_rules! single_value_state_iff_in_append_only {
        () => {
            AggKind::Max | AggKind::Min
        };
    }
    pub use single_value_state_iff_in_append_only;

    /// Ordered-set aggregate functions.
    #[macro_export]
    macro_rules! ordered_set {
        () => {
            AggKind::PercentileCont | AggKind::PercentileDisc | AggKind::Mode
        };
    }
    pub use ordered_set;
}

impl AggKind {
    /// Get the total phase agg kind from the partial phase agg kind.
    pub fn partial_to_total(self) -> Option<Self> {
        match self {
            AggKind::BitAnd
            | AggKind::BitOr
            | AggKind::BitXor
            | AggKind::BoolAnd
            | AggKind::BoolOr
            | AggKind::Min
            | AggKind::Max
            | AggKind::Sum => Some(self),
            AggKind::Sum0 | AggKind::Count => Some(AggKind::Sum0),
            agg_kinds::simply_cannot_two_phase!() => None,
            agg_kinds::rewritten!() => None,
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
    pub fn from_protobuf(args: &[PbInputRef]) -> Result<Self> {
        let args = match args {
            [] => Self::None,
            [arg] => Self::Unary(DataType::from(arg.get_type()?), arg.get_index() as usize),
            [arg1, arg2] => Self::Binary(
                [
                    DataType::from(arg1.get_type()?),
                    DataType::from(arg2.get_type()?),
                ],
                [arg1.get_index() as usize, arg2.get_index() as usize],
            ),
            _ => bail!("too many arguments for agg call"),
        };
        Ok(args)
    }
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
