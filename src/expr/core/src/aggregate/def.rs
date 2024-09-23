// Copyright 2024 RisingWave Labs
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

use std::fmt::Display;
use std::iter::Peekable;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::DatumFromProtoExt;
pub use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
use risingwave_pb::expr::{PbAggCall, PbExprNode, PbInputRef, PbUserDefinedFunctionMetadata};

use crate::expr::{
    build_from_prost, BoxedExpression, ExpectExt, Expression, LiteralExpression, Token,
};
use crate::Result;

/// Represents an aggregation function.
// TODO(runji):
//  remove this struct from the expression module.
//  this module only cares about aggregate functions themselves.
//  advanced features like order by, filter, distinct, etc. should be handled by the upper layer.
#[derive(Debug, Clone)]
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
    pub filter: Option<Arc<dyn Expression>>,

    /// Should deduplicate the input before aggregation.
    pub distinct: bool,

    /// Constant arguments.
    pub direct_args: Vec<LiteralExpression>,
}

impl AggCall {
    pub fn from_protobuf(agg_call: &PbAggCall) -> Result<Self> {
        let agg_kind = AggKind::from_protobuf(
            agg_call.get_kind()?,
            agg_call.udf.as_ref(),
            agg_call.scalar.as_ref(),
        )?;
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
            Some(ref pb_filter) => Some(build_from_prost(pb_filter)?.into()), /* TODO: non-strict filter in streaming */
            None => None,
        };
        let direct_args = agg_call
            .direct_args
            .iter()
            .map(|arg| {
                let data_type = DataType::from(arg.get_type().unwrap());
                LiteralExpression::new(
                    data_type.clone(),
                    Datum::from_protobuf(arg.get_datum().unwrap(), &data_type).unwrap(),
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
            kind: AggKind::from_protobuf(func, None, None).unwrap(),
            args: AggArgs {
                data_types: children.iter().map(|(_, ty)| ty.clone()).collect(),
                val_indices: children.iter().map(|(idx, _)| *idx).collect(),
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

    fn parse_function(&mut self) -> PbAggKind {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(name) => {
                PbAggKind::from_str_name(&name.to_uppercase()).expect_str("function", &name)
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

/// Aggregate function kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumAsInner)]
pub enum AggKind {
    /// Built-in aggregate function.
    ///
    /// The associated value should not be `UserDefined` or `WrapScalar`.
    Builtin(PbAggKind),

    /// User defined aggregate function.
    UserDefined(PbUserDefinedFunctionMetadata),

    /// Wrap a scalar function that takes a list as input as an aggregation function.
    WrapScalar(PbExprNode),
}

impl Display for AggKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Builtin(kind) => write!(f, "{}", kind.as_str_name().to_lowercase()),
            Self::UserDefined(_) => write!(f, "udaf"),
            Self::WrapScalar(_) => write!(f, "wrap_scalar"),
        }
    }
}

/// `FromStr` for builtin aggregate functions.
impl FromStr for AggKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = PbAggKind::from_str(s)?;
        Ok(AggKind::Builtin(kind))
    }
}

impl From<PbAggKind> for AggKind {
    fn from(pb: PbAggKind) -> Self {
        assert!(!matches!(
            pb,
            PbAggKind::Unspecified | PbAggKind::UserDefined | PbAggKind::WrapScalar
        ));
        AggKind::Builtin(pb)
    }
}

impl AggKind {
    pub fn from_protobuf(
        pb_type: PbAggKind,
        user_defined: Option<&PbUserDefinedFunctionMetadata>,
        scalar: Option<&PbExprNode>,
    ) -> Result<Self> {
        match pb_type {
            PbAggKind::UserDefined => {
                let user_defined = user_defined.context("expect user defined")?;
                Ok(AggKind::UserDefined(user_defined.clone()))
            }
            PbAggKind::WrapScalar => {
                let scalar = scalar.context("expect scalar")?;
                Ok(AggKind::WrapScalar(scalar.clone()))
            }
            PbAggKind::Unspecified => bail!("Unrecognized agg."),
            _ => Ok(AggKind::Builtin(pb_type)),
        }
    }

    pub fn to_protobuf(&self) -> PbAggKind {
        match self {
            Self::Builtin(pb) => *pb,
            Self::UserDefined(_) => PbAggKind::UserDefined,
            Self::WrapScalar(_) => PbAggKind::WrapScalar,
        }
    }
}

/// Macros to generate match arms for [`AggKind`](AggKind).
/// IMPORTANT: These macros must be carefully maintained especially when adding new
/// [`AggKind`](AggKind) variants.
pub mod agg_kinds {
    /// [`AggKind`](super::AggKind)s that are currently not supported in streaming mode.
    #[macro_export]
    macro_rules! unimplemented_in_stream {
        () => {
            AggKind::Builtin(
                PbAggKind::PercentileCont | PbAggKind::PercentileDisc | PbAggKind::Mode,
            )
        };
    }
    pub use unimplemented_in_stream;

    /// [`AggKind`](super::AggKind)s that should've been rewritten to other kinds. These kinds
    /// should not appear when generating physical plan nodes.
    #[macro_export]
    macro_rules! rewritten {
        () => {
            AggKind::Builtin(
                PbAggKind::Avg
                    | PbAggKind::StddevPop
                    | PbAggKind::StddevSamp
                    | PbAggKind::VarPop
                    | PbAggKind::VarSamp
                    | PbAggKind::Grouping
                    // ApproxPercentile always uses custom agg executors,
                    // rather than an aggregation operator
                    | PbAggKind::ApproxPercentile
            )
        };
    }
    pub use rewritten;

    /// [`AggKind`](super::AggKind)s of which the aggregate results are not affected by the
    /// user given ORDER BY clause.
    #[macro_export]
    macro_rules! result_unaffected_by_order_by {
        () => {
            AggKind::Builtin(PbAggKind::BitAnd
                | PbAggKind::BitOr
                | PbAggKind::BitXor // XOR is commutative and associative
                | PbAggKind::BoolAnd
                | PbAggKind::BoolOr
                | PbAggKind::Min
                | PbAggKind::Max
                | PbAggKind::Sum
                | PbAggKind::Sum0
                | PbAggKind::Count
                | PbAggKind::Avg
                | PbAggKind::ApproxCountDistinct
                | PbAggKind::VarPop
                | PbAggKind::VarSamp
                | PbAggKind::StddevPop
                | PbAggKind::StddevSamp)
        };
    }
    pub use result_unaffected_by_order_by;

    /// [`AggKind`](super::AggKind)s that must be called with ORDER BY clause. These are
    /// slightly different from variants not in [`result_unaffected_by_order_by`], in that
    /// variants returned by this macro should be banned while the others should just be warned.
    #[macro_export]
    macro_rules! must_have_order_by {
        () => {
            AggKind::Builtin(
                PbAggKind::FirstValue
                    | PbAggKind::LastValue
                    | PbAggKind::PercentileCont
                    | PbAggKind::PercentileDisc
                    | PbAggKind::Mode,
            )
        };
    }
    pub use must_have_order_by;

    /// [`AggKind`](super::AggKind)s of which the aggregate results are not affected by the
    /// user given DISTINCT keyword.
    #[macro_export]
    macro_rules! result_unaffected_by_distinct {
        () => {
            AggKind::Builtin(
                PbAggKind::BitAnd
                    | PbAggKind::BitOr
                    | PbAggKind::BoolAnd
                    | PbAggKind::BoolOr
                    | PbAggKind::Min
                    | PbAggKind::Max
                    | PbAggKind::ApproxCountDistinct,
            )
        };
    }
    pub use result_unaffected_by_distinct;

    /// [`AggKind`](crate::aggregate::AggKind)s that are simply cannot 2-phased.
    #[macro_export]
    macro_rules! simply_cannot_two_phase {
        () => {
            AggKind::Builtin(
                PbAggKind::StringAgg
                    | PbAggKind::ApproxCountDistinct
                    | PbAggKind::ArrayAgg
                    | PbAggKind::JsonbAgg
                    | PbAggKind::JsonbObjectAgg
                    | PbAggKind::FirstValue
                    | PbAggKind::LastValue
                    | PbAggKind::PercentileCont
                    | PbAggKind::PercentileDisc
                    | PbAggKind::Mode
                    // FIXME(wrj): move `BoolAnd` and `BoolOr` out
                    //  after we support general merge in stateless_simple_agg
                    | PbAggKind::BoolAnd
                    | PbAggKind::BoolOr
                    | PbAggKind::BitAnd
                    | PbAggKind::BitOr
            )
            | AggKind::UserDefined(_)
            | AggKind::WrapScalar(_)
        };
    }
    pub use simply_cannot_two_phase;

    /// [`AggKind`](super::AggKind)s that are implemented with a single value state (so-called
    /// stateless).
    #[macro_export]
    macro_rules! single_value_state {
        () => {
            AggKind::Builtin(
                PbAggKind::Sum
                    | PbAggKind::Sum0
                    | PbAggKind::Count
                    | PbAggKind::BitAnd
                    | PbAggKind::BitOr
                    | PbAggKind::BitXor
                    | PbAggKind::BoolAnd
                    | PbAggKind::BoolOr
                    | PbAggKind::ApproxCountDistinct
                    | PbAggKind::InternalLastSeenValue
                    | PbAggKind::ApproxPercentile,
            ) | AggKind::UserDefined(_)
        };
    }
    pub use single_value_state;

    /// [`AggKind`](super::AggKind)s that are implemented with a single value state (so-called
    /// stateless) iff the input is append-only.
    #[macro_export]
    macro_rules! single_value_state_iff_in_append_only {
        () => {
            AggKind::Builtin(PbAggKind::Max | PbAggKind::Min)
        };
    }
    pub use single_value_state_iff_in_append_only;

    /// [`AggKind`](super::AggKind)s that are implemented with a materialized input state.
    #[macro_export]
    macro_rules! materialized_input_state {
        () => {
            AggKind::Builtin(
                PbAggKind::Min
                    | PbAggKind::Max
                    | PbAggKind::FirstValue
                    | PbAggKind::LastValue
                    | PbAggKind::StringAgg
                    | PbAggKind::ArrayAgg
                    | PbAggKind::JsonbAgg
                    | PbAggKind::JsonbObjectAgg,
            ) | AggKind::WrapScalar(_)
        };
    }
    pub use materialized_input_state;

    /// Ordered-set aggregate functions.
    #[macro_export]
    macro_rules! ordered_set {
        () => {
            AggKind::Builtin(
                PbAggKind::PercentileCont
                    | PbAggKind::PercentileDisc
                    | PbAggKind::Mode
                    | PbAggKind::ApproxPercentile,
            )
        };
    }
    pub use ordered_set;
}

impl AggKind {
    /// Get the total phase agg kind from the partial phase agg kind.
    pub fn partial_to_total(&self) -> Option<Self> {
        match self {
            AggKind::Builtin(
                PbAggKind::BitXor
                | PbAggKind::Min
                | PbAggKind::Max
                | PbAggKind::Sum
                | PbAggKind::InternalLastSeenValue,
            ) => Some(self.clone()),
            AggKind::Builtin(PbAggKind::Sum0 | PbAggKind::Count) => {
                Some(Self::Builtin(PbAggKind::Sum0))
            }
            agg_kinds::simply_cannot_two_phase!() => None,
            agg_kinds::rewritten!() => None,
            // invalid variants
            AggKind::Builtin(
                PbAggKind::Unspecified | PbAggKind::UserDefined | PbAggKind::WrapScalar,
            ) => None,
        }
    }
}

/// An aggregation function may accept 0, 1 or 2 arguments.
#[derive(Clone, Debug, Default)]
pub struct AggArgs {
    data_types: Box<[DataType]>,
    val_indices: Box<[usize]>,
}

impl AggArgs {
    pub fn from_protobuf(args: &[PbInputRef]) -> Result<Self> {
        Ok(AggArgs {
            data_types: args
                .iter()
                .map(|arg| DataType::from(arg.get_type().unwrap()))
                .collect(),
            val_indices: args.iter().map(|arg| arg.get_index() as usize).collect(),
        })
    }

    /// return the types of arguments.
    pub fn arg_types(&self) -> &[DataType] {
        &self.data_types
    }

    /// return the indices of the arguments in [`risingwave_common::array::StreamChunk`].
    pub fn val_indices(&self) -> &[usize] {
        &self.val_indices
    }
}

impl FromIterator<(DataType, usize)> for AggArgs {
    fn from_iter<T: IntoIterator<Item = (DataType, usize)>>(iter: T) -> Self {
        let (data_types, val_indices): (Vec<_>, Vec<_>) = iter.into_iter().unzip();
        AggArgs {
            data_types: data_types.into(),
            val_indices: val_indices.into(),
        }
    }
}
