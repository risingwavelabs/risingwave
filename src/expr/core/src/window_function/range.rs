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

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Context;
use educe::Educe;
use futures_util::FutureExt;
use risingwave_common::bail;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Datum, IsNegative, ScalarImpl, ScalarRefImpl, Sentinelled, ToOwnedDatum, ToText,
};
use risingwave_common::util::sort_util::{Direction, OrderType};
use risingwave_common::util::value_encoding::{DatumFromProtoExt, DatumToProtoExt};
use risingwave_pb::expr::window_frame::{PbBoundType, PbRangeFrameBound, PbRangeFrameBounds};

use super::FrameBound::{
    self, CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
};
use super::FrameBoundsImpl;
use crate::Result;
use crate::expr::{
    BoxedExpression, Expression, ExpressionBoxExt, InputRefExpression, LiteralExpression,
    build_func,
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RangeFrameBounds {
    pub order_data_type: DataType,
    pub order_type: OrderType,
    pub offset_data_type: DataType,
    pub start: RangeFrameBound,
    pub end: RangeFrameBound,
}

impl RangeFrameBounds {
    pub(super) fn from_protobuf(bounds: &PbRangeFrameBounds) -> Result<Self> {
        let order_data_type = DataType::from(bounds.get_order_data_type()?);
        let order_type = OrderType::from_protobuf(bounds.get_order_type()?);
        let offset_data_type = DataType::from(bounds.get_offset_data_type()?);
        let start = FrameBound::<RangeFrameOffset>::from_protobuf(
            bounds.get_start()?,
            &order_data_type,
            &offset_data_type,
        )?;
        let end = FrameBound::<RangeFrameOffset>::from_protobuf(
            bounds.get_end()?,
            &order_data_type,
            &offset_data_type,
        )?;
        Ok(Self {
            order_data_type,
            order_type,
            offset_data_type,
            start,
            end,
        })
    }

    pub(super) fn to_protobuf(&self) -> PbRangeFrameBounds {
        PbRangeFrameBounds {
            start: Some(self.start.to_protobuf()),
            end: Some(self.end.to_protobuf()),
            order_data_type: Some(self.order_data_type.to_protobuf()),
            order_type: Some(self.order_type.to_protobuf()),
            offset_data_type: Some(self.offset_data_type.to_protobuf()),
        }
    }
}

impl Display for RangeFrameBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RANGE BETWEEN {} AND {}",
            self.start.for_display(),
            self.end.for_display()
        )?;
        Ok(())
    }
}

impl FrameBoundsImpl for RangeFrameBounds {
    fn validate(&self) -> Result<()> {
        fn validate_non_negative(val: impl IsNegative + Display) -> Result<()> {
            if val.is_negative() {
                bail!(
                    "frame bound offset should be non-negative, but {} is given",
                    val
                );
            }
            Ok(())
        }

        FrameBound::validate_bounds(&self.start, &self.end, |offset| {
            match offset.as_scalar_ref_impl() {
                // TODO(rc): use decl macro?
                ScalarRefImpl::Int16(val) => validate_non_negative(val)?,
                ScalarRefImpl::Int32(val) => validate_non_negative(val)?,
                ScalarRefImpl::Int64(val) => validate_non_negative(val)?,
                ScalarRefImpl::Float32(val) => validate_non_negative(val)?,
                ScalarRefImpl::Float64(val) => validate_non_negative(val)?,
                ScalarRefImpl::Decimal(val) => validate_non_negative(val)?,
                ScalarRefImpl::Interval(val) => {
                    if !val.is_never_negative() {
                        bail!(
                            "for frame bound offset of type `interval`, each field should be non-negative, but {} is given",
                            val
                        );
                    }
                    if matches!(self.order_data_type, DataType::Timestamptz) {
                        // for `timestamptz`, we only support offset without `month` and `day` fields
                        if val.months() != 0 || val.days() != 0 {
                            bail!(
                                "for frame order column of type `timestamptz`, offset should not have non-zero `month` and `day`",
                            );
                        }
                    }
                }
                _ => unreachable!(
                    "other order column data types are not supported and should be banned in frontend"
                ),
            }
            Ok(())
        })
    }
}

impl RangeFrameBounds {
    /// Get the frame start for a given order column value.
    ///
    /// ## Examples
    ///
    /// For the following frames:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// ORDER BY x DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// ```
    ///
    /// For any CURRENT ROW with any order value, the frame start is always the first-most row, which is
    /// represented by [`Sentinelled::Smallest`].
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
    /// ```
    ///
    /// For CURRENT ROW with order value `100`, the frame start is the **FIRST** row with order value `90`.
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
    /// ```
    ///
    /// For CURRENT ROW with order value `100`, the frame start is the **FIRST** row with order value `110`.
    pub fn frame_start_of(&self, order_value: impl ToOwnedDatum) -> Sentinelled<Datum> {
        self.start.for_calc().bound_of(order_value, self.order_type)
    }

    /// Get the frame end for a given order column value. It's very similar to `frame_start_of`, just with
    /// everything on the other direction.
    pub fn frame_end_of(&self, order_value: impl ToOwnedDatum) -> Sentinelled<Datum> {
        self.end.for_calc().bound_of(order_value, self.order_type)
    }

    /// Get the order value of the CURRENT ROW of the first frame that includes the given order value.
    ///
    /// ## Examples
    ///
    /// For the following frames:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    /// ORDER BY x DESC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    /// ```
    ///
    /// For any given order value, the first CURRENT ROW is always the first-most row, which is
    /// represented by [`Sentinelled::Smallest`].
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING
    /// ```
    ///
    /// For a given order value `100`, the first CURRENT ROW should have order value `90`.
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x DESC RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING
    /// ```
    ///
    /// For a given order value `100`, the first CURRENT ROW should have order value `110`.
    pub fn first_curr_of(&self, order_value: impl ToOwnedDatum) -> Sentinelled<Datum> {
        self.end
            .for_calc()
            .reverse()
            .bound_of(order_value, self.order_type)
    }

    /// Get the order value of the CURRENT ROW of the last frame that includes the given order value.
    /// It's very similar to `first_curr_of`, just with everything on the other direction.
    pub fn last_curr_of(&self, order_value: impl ToOwnedDatum) -> Sentinelled<Datum> {
        self.start
            .for_calc()
            .reverse()
            .bound_of(order_value, self.order_type)
    }
}

pub type RangeFrameBound = FrameBound<RangeFrameOffset>;

impl RangeFrameBound {
    fn from_protobuf(
        bound: &PbRangeFrameBound,
        order_data_type: &DataType,
        offset_data_type: &DataType,
    ) -> Result<Self> {
        let bound = match bound.get_type()? {
            PbBoundType::Unspecified => bail!("unspecified type of `RangeFrameBound`"),
            PbBoundType::UnboundedPreceding => Self::UnboundedPreceding,
            PbBoundType::CurrentRow => Self::CurrentRow,
            PbBoundType::UnboundedFollowing => Self::UnboundedFollowing,
            bound_type @ (PbBoundType::Preceding | PbBoundType::Following) => {
                let offset_value = Datum::from_protobuf(bound.get_offset()?, offset_data_type)
                    .context("offset `Datum` is not decodable")?
                    .context("offset of `RangeFrameBound` must be non-NULL")?;
                let mut offset = RangeFrameOffset::new(offset_value);
                offset.prepare(order_data_type, offset_data_type)?;
                if bound_type == PbBoundType::Preceding {
                    Self::Preceding(offset)
                } else {
                    Self::Following(offset)
                }
            }
        };
        Ok(bound)
    }

    fn to_protobuf(&self) -> PbRangeFrameBound {
        let (r#type, offset) = match self {
            Self::UnboundedPreceding => (PbBoundType::UnboundedPreceding, None),
            Self::Preceding(offset) => (
                PbBoundType::Preceding,
                Some(Some(offset.as_scalar_ref_impl()).to_protobuf()),
            ),
            Self::CurrentRow => (PbBoundType::CurrentRow, None),
            Self::Following(offset) => (
                PbBoundType::Following,
                Some(Some(offset.as_scalar_ref_impl()).to_protobuf()),
            ),
            Self::UnboundedFollowing => (PbBoundType::UnboundedFollowing, None),
        };
        PbRangeFrameBound {
            r#type: r#type as _,
            offset,
        }
    }
}

impl RangeFrameBound {
    fn for_display(&self) -> FrameBound<String> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(offset.as_scalar_ref_impl().to_text()),
            CurrentRow => CurrentRow,
            Following(offset) => Following(offset.as_scalar_ref_impl().to_text()),
            UnboundedFollowing => UnboundedFollowing,
        }
    }

    fn for_calc(&self) -> FrameBound<RangeFrameOffsetRef<'_>> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(RangeFrameOffsetRef {
                add_expr: offset.add_expr.as_ref().unwrap().as_ref(),
                sub_expr: offset.sub_expr.as_ref().unwrap().as_ref(),
            }),
            CurrentRow => CurrentRow,
            Following(offset) => Following(RangeFrameOffsetRef {
                add_expr: offset.add_expr.as_ref().unwrap().as_ref(),
                sub_expr: offset.sub_expr.as_ref().unwrap().as_ref(),
            }),
            UnboundedFollowing => UnboundedFollowing,
        }
    }
}

/// The wrapper type for [`ScalarImpl`] range frame offset, containing
/// two expressions to help adding and subtracting the offset.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct RangeFrameOffset {
    /// The original offset value.
    offset: ScalarImpl,
    /// Built expression for `$0 + offset`.
    #[educe(PartialEq(ignore), Hash(ignore))]
    add_expr: Option<Arc<BoxedExpression>>,
    /// Built expression for `$0 - offset`.
    #[educe(PartialEq(ignore), Hash(ignore))]
    sub_expr: Option<Arc<BoxedExpression>>,
}

impl RangeFrameOffset {
    pub fn new(offset: ScalarImpl) -> Self {
        Self {
            offset,
            add_expr: None,
            sub_expr: None,
        }
    }

    fn prepare(&mut self, order_data_type: &DataType, offset_data_type: &DataType) -> Result<()> {
        use risingwave_pb::expr::expr_node::PbType as PbExprType;

        let input_expr = InputRefExpression::new(order_data_type.clone(), 0);
        let offset_expr =
            LiteralExpression::new(offset_data_type.clone(), Some(self.offset.clone()));
        self.add_expr = Some(Arc::new(build_func(
            PbExprType::Add,
            order_data_type.clone(),
            vec![input_expr.clone().boxed(), offset_expr.clone().boxed()],
        )?));
        self.sub_expr = Some(Arc::new(build_func(
            PbExprType::Subtract,
            order_data_type.clone(),
            vec![input_expr.boxed(), offset_expr.boxed()],
        )?));
        Ok(())
    }

    pub fn new_for_test(
        offset: ScalarImpl,
        order_data_type: &DataType,
        offset_data_type: &DataType,
    ) -> Self {
        let mut offset = Self::new(offset);
        offset.prepare(order_data_type, offset_data_type).unwrap();
        offset
    }
}

impl Deref for RangeFrameOffset {
    type Target = ScalarImpl;

    fn deref(&self) -> &Self::Target {
        &self.offset
    }
}

#[derive(Debug, Educe)]
#[educe(Clone, Copy)]
struct RangeFrameOffsetRef<'a> {
    /// Built expression for `$0 + offset`.
    add_expr: &'a dyn Expression,
    /// Built expression for `$0 - offset`.
    sub_expr: &'a dyn Expression,
}

impl FrameBound<RangeFrameOffsetRef<'_>> {
    fn bound_of(self, order_value: impl ToOwnedDatum, order_type: OrderType) -> Sentinelled<Datum> {
        let expr = match (self, order_type.direction()) {
            (UnboundedPreceding, _) => return Sentinelled::Smallest,
            (UnboundedFollowing, _) => return Sentinelled::Largest,
            (CurrentRow, _) => return Sentinelled::Normal(order_value.to_owned_datum()),
            (Preceding(offset), Direction::Ascending)
            | (Following(offset), Direction::Descending) => {
                // should SUBTRACT the offset
                offset.sub_expr
            }
            (Following(offset), Direction::Ascending)
            | (Preceding(offset), Direction::Descending) => {
                // should ADD the offset
                offset.add_expr
            }
        };
        let row = OwnedRow::new(vec![order_value.to_owned_datum()]);
        Sentinelled::Normal(
            expr.eval_row(&row)
                .now_or_never()
                .expect("frame bound calculation should finish immediately")
                .expect("just simple calculation, should succeed"), // TODO(rc): handle overflow
        )
    }
}
