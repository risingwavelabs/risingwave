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

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Context;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use futures_util::FutureExt;
use parse_display::Display;
use risingwave_common::bail;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Datum, IsNegative, ScalarImpl, ScalarRefImpl, Sentinelled, ToOwnedDatum, ToText,
};
use risingwave_common::util::sort_util::{Direction, OrderType};
use risingwave_common::util::value_encoding::{DatumFromProtoExt, DatumToProtoExt};
use risingwave_pb::expr::window_frame::{PbBound, PbExclusion};
use risingwave_pb::expr::{PbWindowFrame, PbWindowFunction};
use FrameBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};

use super::WindowFuncKind;
use crate::aggregate::AggArgs;
use crate::expr::{
    build_func, BoxedExpression, Expression, ExpressionBoxExt, InputRefExpression,
    LiteralExpression,
};
use crate::Result;

#[derive(Debug, Clone)]
pub struct WindowFuncCall {
    pub kind: WindowFuncKind,
    pub args: AggArgs,
    pub return_type: DataType,
    pub frame: Frame,
}

impl WindowFuncCall {
    pub fn from_protobuf(call: &PbWindowFunction) -> Result<Self> {
        let call = WindowFuncCall {
            kind: WindowFuncKind::from_protobuf(call.get_type()?)?,
            args: AggArgs::from_protobuf(call.get_args())?,
            return_type: DataType::from(call.get_return_type()?),
            frame: Frame::from_protobuf(call.get_frame()?)?,
        };
        Ok(call)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Frame {
    pub bounds: FrameBounds,
    pub exclusion: FrameExclusion,
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.bounds)?;
        if self.exclusion != FrameExclusion::default() {
            write!(f, " {}", self.exclusion)?;
        }
        Ok(())
    }
}

impl Frame {
    pub fn rows(start: FrameBound<usize>, end: FrameBound<usize>) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion: FrameExclusion::default(),
        }
    }

    pub fn rows_with_exclusion(
        start: FrameBound<usize>,
        end: FrameBound<usize>,
        exclusion: FrameExclusion,
    ) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion,
        }
    }
}

impl Frame {
    pub fn from_protobuf(frame: &PbWindowFrame) -> Result<Self> {
        use risingwave_pb::expr::window_frame::PbType;
        let start = frame.get_start()?;
        let end = frame.get_end()?;
        let bounds = match frame.get_type()? {
            PbType::Unspecified => bail!("unspecified type of `WindowFrame`"),
            PbType::Rows => {
                let start = FrameBound::<usize>::from_protobuf(start)?;
                let end = FrameBound::<usize>::from_protobuf(end)?;
                FrameBounds::Rows(RowsFrameBounds { start, end })
            }
            PbType::Range => {
                let order_data_type = DataType::from(frame.get_order_data_type()?);
                let order_type = OrderType::from_protobuf(frame.get_order_type()?);
                let offset_data_type = DataType::from(frame.get_offset_data_type()?);
                let start = FrameBound::<RangeFrameOffset>::from_protobuf(
                    start,
                    &order_data_type,
                    &offset_data_type,
                )?;
                let end = FrameBound::<RangeFrameOffset>::from_protobuf(
                    end,
                    &order_data_type,
                    &offset_data_type,
                )?;
                FrameBounds::Range(RangeFrameBounds {
                    order_data_type,
                    order_type,
                    offset_data_type,
                    start,
                    end,
                })
            }
        };
        let exclusion = FrameExclusion::from_protobuf(frame.get_exclusion()?)?;
        Ok(Self { bounds, exclusion })
    }

    pub fn to_protobuf(&self) -> PbWindowFrame {
        use risingwave_pb::expr::window_frame::PbType;
        let exclusion = self.exclusion.to_protobuf() as _;
        match &self.bounds {
            FrameBounds::Rows(RowsFrameBounds { start, end }) => PbWindowFrame {
                r#type: PbType::Rows as _,
                start: Some(start.to_protobuf()),
                end: Some(end.to_protobuf()),
                exclusion,
                order_data_type: None,
                order_type: None,
                offset_data_type: None,
            },
            FrameBounds::Range(RangeFrameBounds {
                order_data_type,
                order_type,
                offset_data_type,
                start,
                end,
            }) => PbWindowFrame {
                r#type: PbType::Range as _,
                start: Some(start.to_protobuf()),
                end: Some(end.to_protobuf()),
                exclusion,
                order_data_type: Some(order_data_type.to_protobuf()),
                order_type: Some(order_type.to_protobuf()),
                offset_data_type: Some(offset_data_type.to_protobuf()),
            },
        }
    }
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash, EnumAsInner)]
#[display("{0}")]
pub enum FrameBounds {
    Rows(RowsFrameBounds),
    // Groups(GroupsFrameBounds),
    Range(RangeFrameBounds),
}

impl FrameBounds {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Rows(bounds) => bounds.validate(),
            Self::Range(bounds) => bounds.validate(),
        }
    }

    pub fn start_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { start, .. }) => start.is_unbounded_preceding(),
            Self::Range(RangeFrameBounds { start, .. }) => start.is_unbounded_preceding(),
        }
    }

    pub fn end_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { end, .. }) => end.is_unbounded_following(),
            Self::Range(RangeFrameBounds { end, .. }) => end.is_unbounded_following(),
        }
    }

    pub fn is_unbounded(&self) -> bool {
        self.start_is_unbounded() || self.end_is_unbounded()
    }
}

pub trait FrameBoundsImpl {
    fn validate(&self) -> Result<()>;
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash)]
#[display("ROWS BETWEEN {start} AND {end}")]
pub struct RowsFrameBounds {
    pub start: FrameBound<usize>,
    pub end: FrameBound<usize>,
}

impl RowsFrameBounds {
    /// Check if the `ROWS` frame is canonical.
    ///
    /// A canonical `ROWS` frame is defined as:
    ///
    /// - It is valid.
    /// - It is a crossing frame, i.e. it contains the current row.
    pub fn is_canonical(&self) -> bool {
        self.validate().is_ok() && {
            let start = self.start.to_offset();
            let end = self.end.to_offset();
            start.unwrap_or(0) <= 0 && end.unwrap_or(0) >= 0
        }
    }
}

impl FrameBoundsImpl for RowsFrameBounds {
    fn validate(&self) -> Result<()> {
        FrameBound::validate_bounds(&self.start, &self.end, |_| Ok(()))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RangeFrameBounds {
    pub order_data_type: DataType,
    pub order_type: OrderType,
    pub offset_data_type: DataType,
    pub start: FrameBound<RangeFrameOffset>,
    pub end: FrameBound<RangeFrameOffset>,
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
}

impl Deref for RangeFrameOffset {
    type Target = ScalarImpl;

    fn deref(&self) -> &Self::Target {
        &self.offset
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
                },
                _ => unreachable!("other order column data types are not supported and should be banned in frontend"),
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

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash, EnumAsInner)]
#[display(style = "TITLE CASE")]
pub enum FrameBound<T> {
    UnboundedPreceding,
    #[display("{0} PRECEDING")]
    Preceding(T),
    CurrentRow,
    #[display("{0} FOLLOWING")]
    Following(T),
    UnboundedFollowing,
}

impl<T> FrameBound<T> {
    fn offset_value(&self) -> Option<&T> {
        match self {
            UnboundedPreceding | UnboundedFollowing | CurrentRow => None,
            Preceding(offset) | Following(offset) => Some(offset),
        }
    }

    fn validate_bounds(
        start: &Self,
        end: &Self,
        offset_checker: impl Fn(&T) -> Result<()>,
    ) -> Result<()> {
        match (start, end) {
            (_, UnboundedPreceding) => bail!("frame end cannot be UNBOUNDED PRECEDING"),
            (UnboundedFollowing, _) => {
                bail!("frame start cannot be UNBOUNDED FOLLOWING")
            }
            (Following(_), CurrentRow) | (Following(_), Preceding(_)) => {
                bail!("frame starting from following row cannot have preceding rows")
            }
            (CurrentRow, Preceding(_)) => {
                bail!("frame starting from current row cannot have preceding rows")
            }
            _ => {}
        }

        for bound in [start, end] {
            if let Some(offset) = bound.offset_value() {
                offset_checker(offset)?;
            }
        }

        Ok(())
    }

    pub fn map<U>(self, f: impl Fn(T) -> U) -> FrameBound<U> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(f(offset)),
            CurrentRow => CurrentRow,
            Following(offset) => Following(f(offset)),
            UnboundedFollowing => UnboundedFollowing,
        }
    }
}

impl<T> FrameBound<T>
where
    T: Copy,
{
    fn reverse(self) -> FrameBound<T> {
        match self {
            UnboundedPreceding => UnboundedFollowing,
            Preceding(offset) => Following(offset),
            CurrentRow => CurrentRow,
            Following(offset) => Preceding(offset),
            UnboundedFollowing => UnboundedPreceding,
        }
    }
}

impl FrameBound<usize> {
    pub fn from_protobuf(bound: &PbBound) -> Result<Self> {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let offset = bound.get_offset()?;
        let bound = match offset {
            PbOffset::Integer(offset) => match bound.get_type()? {
                PbBoundType::Unspecified => bail!("unspecified type of `FrameBound<usize>`"),
                PbBoundType::UnboundedPreceding => Self::UnboundedPreceding,
                PbBoundType::Preceding => Self::Preceding(*offset as usize),
                PbBoundType::CurrentRow => Self::CurrentRow,
                PbBoundType::Following => Self::Following(*offset as usize),
                PbBoundType::UnboundedFollowing => Self::UnboundedFollowing,
            },
            PbOffset::Datum(_) => bail!("offset of `FrameBound<usize>` must be `Integer`"),
        };
        Ok(bound)
    }

    pub fn to_protobuf(&self) -> PbBound {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let (r#type, offset) = match self {
            Self::UnboundedPreceding => (PbBoundType::UnboundedPreceding, PbOffset::Integer(0)),
            Self::Preceding(offset) => (PbBoundType::Preceding, PbOffset::Integer(*offset as _)),
            Self::CurrentRow => (PbBoundType::CurrentRow, PbOffset::Integer(0)),
            Self::Following(offset) => (PbBoundType::Following, PbOffset::Integer(*offset as _)),
            Self::UnboundedFollowing => (PbBoundType::UnboundedFollowing, PbOffset::Integer(0)),
        };
        PbBound {
            r#type: r#type as _,
            offset: Some(offset),
        }
    }
}

impl FrameBound<usize> {
    /// Convert the bound to sized offset from current row. `None` if the bound is unbounded.
    pub fn to_offset(&self) -> Option<isize> {
        match self {
            UnboundedPreceding | UnboundedFollowing => None,
            CurrentRow => Some(0),
            Preceding(n) => Some(-(*n as isize)),
            Following(n) => Some(*n as isize),
        }
    }

    /// View the bound as frame start, and get the number of preceding rows.
    pub fn n_preceding_rows(&self) -> Option<usize> {
        match self {
            UnboundedPreceding => None,
            Preceding(n) => Some(*n),
            CurrentRow | Following(_) | UnboundedFollowing => Some(0),
        }
    }

    /// View the bound as frame end, and get the number of following rows.
    pub fn n_following_rows(&self) -> Option<usize> {
        match self {
            UnboundedFollowing => None,
            Following(n) => Some(*n),
            CurrentRow | Preceding(_) | UnboundedPreceding => Some(0),
        }
    }
}

impl FrameBound<RangeFrameOffset> {
    pub fn from_protobuf(
        bound: &PbBound,
        order_data_type: &DataType,
        offset_data_type: &DataType,
    ) -> Result<Self> {
        use risingwave_pb::expr::expr_node::PbType as PbExprType;
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let offset = bound.get_offset()?;
        let bound = match offset {
            PbOffset::Integer(_) => bail!("offset of `RANGE` frame bound must be `Datum`"),
            PbOffset::Datum(offset) => match bound.get_type()? {
                PbBoundType::Unspecified => bail!("unspecified type of `FrameBound<ScalarImpl>`"),
                PbBoundType::UnboundedPreceding => Self::UnboundedPreceding,
                PbBoundType::CurrentRow => Self::CurrentRow,
                PbBoundType::UnboundedFollowing => Self::UnboundedFollowing,
                bound_type @ (PbBoundType::Preceding | PbBoundType::Following) => {
                    let offset_value = Datum::from_protobuf(offset, offset_data_type)
                        .context("offset `Datum` is not decodable")?
                        .context("offset of `FrameBound<ScalarImpl>` must be non-NULL")?;
                    let input_expr = InputRefExpression::new(order_data_type.clone(), 0);
                    let offset_expr = LiteralExpression::new(
                        offset_data_type.clone(),
                        Some(offset_value.clone()),
                    );
                    let add_expr = build_func(
                        PbExprType::Add,
                        order_data_type.clone(),
                        vec![input_expr.clone().boxed(), offset_expr.clone().boxed()],
                    )?;
                    let sub_expr = build_func(
                        PbExprType::Subtract,
                        order_data_type.clone(),
                        vec![input_expr.boxed(), offset_expr.boxed()],
                    )?;
                    let offset = RangeFrameOffset {
                        offset: offset_value,
                        add_expr: Some(Arc::new(add_expr)),
                        sub_expr: Some(Arc::new(sub_expr)),
                    };
                    if bound_type == PbBoundType::Preceding {
                        Self::Preceding(offset)
                    } else {
                        Self::Following(offset)
                    }
                }
            },
        };
        Ok(bound)
    }

    pub fn to_protobuf(&self) -> PbBound {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let (r#type, offset) = match self {
            Self::UnboundedPreceding => (
                PbBoundType::UnboundedPreceding,
                PbOffset::Datum(Default::default()),
            ),
            Self::Preceding(offset) => (
                PbBoundType::Preceding,
                PbOffset::Datum(Some(offset.as_scalar_ref_impl()).to_protobuf()),
            ),
            Self::CurrentRow => (PbBoundType::CurrentRow, PbOffset::Datum(Default::default())),
            Self::Following(offset) => (
                PbBoundType::Following,
                PbOffset::Datum(Some(offset.as_scalar_ref_impl()).to_protobuf()),
            ),
            Self::UnboundedFollowing => (
                PbBoundType::UnboundedFollowing,
                PbOffset::Datum(Default::default()),
            ),
        };
        PbBound {
            r#type: r#type as _,
            offset: Some(offset),
        }
    }
}

impl FrameBound<RangeFrameOffset> {
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

#[derive(Debug, Educe)]
#[educe(Clone, Copy)]
pub struct RangeFrameOffsetRef<'a> {
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

#[derive(Display, Debug, Copy, Clone, Eq, PartialEq, Hash, Default, EnumAsInner)]
#[display("EXCLUDE {}", style = "TITLE CASE")]
pub enum FrameExclusion {
    CurrentRow,
    // Group,
    // Ties,
    #[default]
    NoOthers,
}

impl FrameExclusion {
    pub fn from_protobuf(exclusion: PbExclusion) -> Result<Self> {
        let excl = match exclusion {
            PbExclusion::Unspecified => bail!("unspecified type of `FrameExclusion`"),
            PbExclusion::CurrentRow => Self::CurrentRow,
            PbExclusion::NoOthers => Self::NoOthers,
        };
        Ok(excl)
    }

    pub fn to_protobuf(self) -> PbExclusion {
        match self {
            Self::CurrentRow => PbExclusion::CurrentRow,
            Self::NoOthers => PbExclusion::NoOthers,
        }
    }
}
