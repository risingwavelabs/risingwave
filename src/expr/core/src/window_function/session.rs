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
use futures::FutureExt;
use risingwave_common::bail;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Datum, IsNegative, ScalarImpl, ScalarRefImpl, ToOwnedDatum, ToText,
};
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{DatumFromProtoExt, DatumToProtoExt};
use risingwave_pb::expr::window_frame::PbSessionFrameBounds;

use super::FrameBoundsImpl;
use crate::Result;
use crate::expr::{
    BoxedExpression, Expression, ExpressionBoxExt, InputRefExpression, LiteralExpression,
    build_func,
};

/// To implement Session Window in a similar way to Range Frame, we define a similar frame bounds
/// structure here. It's very like [`RangeFrameBounds`](super::RangeFrameBounds), but with a gap
/// instead of start & end offset.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SessionFrameBounds {
    pub order_data_type: DataType,
    pub order_type: OrderType,
    pub gap_data_type: DataType,
    pub gap: SessionFrameGap,
}

impl SessionFrameBounds {
    pub(super) fn from_protobuf(bounds: &PbSessionFrameBounds) -> Result<Self> {
        let order_data_type = DataType::from(bounds.get_order_data_type()?);
        let order_type = OrderType::from_protobuf(bounds.get_order_type()?);
        let gap_data_type = DataType::from(bounds.get_gap_data_type()?);
        let gap_value = Datum::from_protobuf(bounds.get_gap()?, &gap_data_type)
            .context("gap `Datum` is not decodable")?
            .context("gap of session frame must be non-NULL")?;
        let mut gap = SessionFrameGap::new(gap_value);
        gap.prepare(&order_data_type, &gap_data_type)?;
        Ok(Self {
            order_data_type,
            order_type,
            gap_data_type,
            gap,
        })
    }

    pub(super) fn to_protobuf(&self) -> PbSessionFrameBounds {
        PbSessionFrameBounds {
            gap: Some(Some(self.gap.as_scalar_ref_impl()).to_protobuf()),
            order_data_type: Some(self.order_data_type.to_protobuf()),
            order_type: Some(self.order_type.to_protobuf()),
            gap_data_type: Some(self.gap_data_type.to_protobuf()),
        }
    }
}

impl Display for SessionFrameBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SESSION WITH GAP {}",
            self.gap.as_scalar_ref_impl().to_text()
        )
    }
}

impl FrameBoundsImpl for SessionFrameBounds {
    fn validate(&self) -> Result<()> {
        // TODO(rc): maybe can merge with `RangeFrameBounds::validate`

        fn validate_non_negative(val: impl IsNegative + Display) -> Result<()> {
            if val.is_negative() {
                bail!("session gap should be non-negative, but {} is given", val);
            }
            Ok(())
        }

        match self.gap.as_scalar_ref_impl() {
            ScalarRefImpl::Int16(val) => validate_non_negative(val)?,
            ScalarRefImpl::Int32(val) => validate_non_negative(val)?,
            ScalarRefImpl::Int64(val) => validate_non_negative(val)?,
            ScalarRefImpl::Float32(val) => validate_non_negative(val)?,
            ScalarRefImpl::Float64(val) => validate_non_negative(val)?,
            ScalarRefImpl::Decimal(val) => validate_non_negative(val)?,
            ScalarRefImpl::Interval(val) => {
                if !val.is_never_negative() {
                    bail!(
                        "for session gap of type `interval`, each field should be non-negative, but {} is given",
                        val
                    );
                }
                if matches!(self.order_data_type, DataType::Timestamptz) {
                    // for `timestamptz`, we only support gap without `month` and `day` fields
                    if val.months() != 0 || val.days() != 0 {
                        bail!(
                            "for session order column of type `timestamptz`, gap should not have non-zero `month` and `day`",
                        );
                    }
                }
            }
            _ => unreachable!(
                "other order column data types are not supported and should be banned in frontend"
            ),
        }
        Ok(())
    }
}

impl SessionFrameBounds {
    pub fn minimal_next_start_of(&self, end_order_value: impl ToOwnedDatum) -> Datum {
        self.gap.for_calc().minimal_next_start_of(end_order_value)
    }
}

/// The wrapper type for [`ScalarImpl`] session gap, containing an expression to help adding the gap
/// to a given value.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SessionFrameGap {
    /// The original gap value.
    gap: ScalarImpl,
    /// Built expression for `$0 + gap`.
    #[educe(PartialEq(ignore), Hash(ignore))]
    add_expr: Option<Arc<BoxedExpression>>,
}

impl Deref for SessionFrameGap {
    type Target = ScalarImpl;

    fn deref(&self) -> &Self::Target {
        &self.gap
    }
}

impl SessionFrameGap {
    pub fn new(gap: ScalarImpl) -> Self {
        Self {
            gap,
            add_expr: None,
        }
    }

    fn prepare(&mut self, order_data_type: &DataType, gap_data_type: &DataType) -> Result<()> {
        use risingwave_pb::expr::expr_node::PbType as PbExprType;

        let input_expr = InputRefExpression::new(order_data_type.clone(), 0);
        let gap_expr = LiteralExpression::new(gap_data_type.clone(), Some(self.gap.clone()));
        self.add_expr = Some(Arc::new(build_func(
            PbExprType::Add,
            order_data_type.clone(),
            vec![input_expr.clone().boxed(), gap_expr.clone().boxed()],
        )?));
        Ok(())
    }

    pub fn new_for_test(
        gap: ScalarImpl,
        order_data_type: &DataType,
        gap_data_type: &DataType,
    ) -> Self {
        let mut gap = Self::new(gap);
        gap.prepare(order_data_type, gap_data_type).unwrap();
        gap
    }

    fn for_calc(&self) -> SessionFrameGapRef<'_> {
        SessionFrameGapRef {
            add_expr: self.add_expr.as_ref().unwrap().as_ref(),
        }
    }
}

#[derive(Debug, Educe)]
#[educe(Clone, Copy)]
struct SessionFrameGapRef<'a> {
    add_expr: &'a dyn Expression,
}

impl SessionFrameGapRef<'_> {
    fn minimal_next_start_of(&self, end_order_value: impl ToOwnedDatum) -> Datum {
        let row = OwnedRow::new(vec![end_order_value.to_owned_datum()]);
        self.add_expr
            .eval_row(&row)
            .now_or_never()
            .expect("frame bound calculation should finish immediately")
            .expect("just simple calculation, should succeed") // TODO(rc): handle overflow
    }
}
