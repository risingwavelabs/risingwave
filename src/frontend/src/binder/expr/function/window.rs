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

use itertools::Itertools;
use risingwave_common::types::{DataType, ScalarImpl, data_types};
use risingwave_common::{bail_not_implemented, must_match};
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_expr::window_function::{
    Frame, FrameBound, FrameBounds, FrameExclusion, RangeFrameBounds, RangeFrameOffset,
    RowsFrameBounds, SessionFrameBounds, SessionFrameGap, WindowFuncKind,
};
use risingwave_sqlparser::ast::{
    self, WindowFrameBound, WindowFrameBounds, WindowFrameExclusion, WindowFrameUnits, WindowSpec,
};

use crate::Binder;
use crate::binder::Clause;
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr, ExprImpl, OrderBy, WindowFunction};

impl Binder {
    fn ensure_window_function_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::Where
                | Clause::Values
                | Clause::GroupBy
                | Clause::Having
                | Clause::Filter
                | Clause::GeneratedColumn
                | Clause::From
                | Clause::Insert
                | Clause::JoinOn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "window functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Bind window function calls according to PostgreSQL syntax.
    /// See <https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS> for syntax detail.
    pub(super) fn bind_window_function(
        &mut self,
        kind: WindowFuncKind,
        args: Vec<ExprImpl>,
        ignore_nulls: bool,
        filter: Option<Box<ast::Expr>>,
        WindowSpec {
            partition_by,
            order_by,
            window_frame,
        }: WindowSpec,
    ) -> Result<ExprImpl> {
        self.ensure_window_function_allowed()?;

        if ignore_nulls {
            match &kind {
                WindowFuncKind::Aggregate(AggType::Builtin(
                    PbAggKind::FirstValue | PbAggKind::LastValue,
                )) => {
                    // pass
                }
                WindowFuncKind::Lag | WindowFuncKind::Lead => {
                    bail_not_implemented!("`IGNORE NULLS` is not supported for `{}` yet", kind);
                }
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "`IGNORE NULLS` is not allowed for `{}`",
                        kind
                    ))
                    .into());
                }
            }
        }

        if filter.is_some() {
            bail_not_implemented!("`FILTER` is not supported yet");
        }

        let partition_by = partition_by
            .into_iter()
            .map(|arg| self.bind_expr_inner(arg))
            .try_collect()?;
        let order_by = OrderBy::new(
            order_by
                .into_iter()
                .map(|order_by_expr| self.bind_order_by_expr(order_by_expr))
                .collect::<Result<_>>()?,
        );
        let frame = if let Some(frame) = window_frame {
            let exclusion = if let Some(exclusion) = frame.exclusion {
                match exclusion {
                    WindowFrameExclusion::CurrentRow => FrameExclusion::CurrentRow,
                    WindowFrameExclusion::Group | WindowFrameExclusion::Ties => {
                        bail_not_implemented!(
                            issue = 9124,
                            "window frame exclusion `{}` is not supported yet",
                            exclusion
                        );
                    }
                    WindowFrameExclusion::NoOthers => FrameExclusion::NoOthers,
                }
            } else {
                FrameExclusion::NoOthers
            };
            let bounds = match frame.units {
                WindowFrameUnits::Rows => {
                    let (start, end) = must_match!(frame.bounds, WindowFrameBounds::Bounds { start, end } => (start, end));
                    let (start, end) = self.bind_window_frame_usize_bounds(start, end)?;
                    FrameBounds::Rows(RowsFrameBounds { start, end })
                }
                unit @ (WindowFrameUnits::Range | WindowFrameUnits::Session) => {
                    let order_by_expr = order_by
                        .sort_exprs
                        .iter()
                        // for `RANGE | SESSION` frame, there should be exactly one `ORDER BY` column
                        .exactly_one()
                        .map_err(|_| {
                            ErrorCode::InvalidInputSyntax(format!(
                                "there should be exactly one ordering column for `{}` frame",
                                unit
                            ))
                        })?;
                    let order_data_type = order_by_expr.expr.return_type();
                    let order_type = order_by_expr.order_type;

                    let offset_data_type = match &order_data_type {
                        // for numeric ordering columns, `offset`/`gap` should be the same type
                        // NOTE: actually in PG it can be a larger type, but we don't support this here
                        t @ data_types::range_frame_numeric!() => t.clone(),
                        // for datetime ordering columns, `offset`/`gap` should be interval
                        t @ data_types::range_frame_datetime!() => {
                            if matches!(t, DataType::Date | DataType::Time) {
                                bail_not_implemented!(
                                    "`{}` frame with offset of type `{}` is not implemented yet, please manually cast the `ORDER BY` column to `timestamp`",
                                    unit,
                                    t
                                );
                            }
                            DataType::Interval
                        }
                        // other types are not supported
                        t => {
                            return Err(ErrorCode::NotSupported(
                                format!(
                                    "`{}` frame with offset of type `{}` is not supported",
                                    unit, t
                                ),
                                "Please re-consider the `ORDER BY` column".to_owned(),
                            )
                            .into());
                        }
                    };

                    if unit == WindowFrameUnits::Range {
                        let (start, end) = must_match!(frame.bounds, WindowFrameBounds::Bounds { start, end } => (start, end));
                        let (start, end) = self.bind_window_frame_scalar_impl_bounds(
                            start,
                            end,
                            &offset_data_type,
                        )?;
                        FrameBounds::Range(RangeFrameBounds {
                            order_data_type,
                            order_type,
                            offset_data_type,
                            start: start.map(RangeFrameOffset::new),
                            end: end.map(RangeFrameOffset::new),
                        })
                    } else {
                        let gap = must_match!(frame.bounds, WindowFrameBounds::Gap(gap) => gap);
                        let gap_value =
                            self.bind_window_frame_bound_offset(*gap, offset_data_type.clone())?;
                        FrameBounds::Session(SessionFrameBounds {
                            order_data_type,
                            order_type,
                            gap_data_type: offset_data_type,
                            gap: SessionFrameGap::new(gap_value),
                        })
                    }
                }
                WindowFrameUnits::Groups => {
                    bail_not_implemented!(
                        issue = 9124,
                        "window frame in `GROUPS` mode is not supported yet",
                    );
                }
            };

            // Validate the frame bounds, may return `ExprError` to user if the bounds given are not valid.
            bounds.validate()?;

            Some(Frame { bounds, exclusion })
        } else {
            None
        };
        Ok(WindowFunction::new(kind, args, ignore_nulls, partition_by, order_by, frame)?.into())
    }

    fn bind_window_frame_usize_bounds(
        &mut self,
        start: WindowFrameBound,
        end: Option<WindowFrameBound>,
    ) -> Result<(FrameBound<usize>, FrameBound<usize>)> {
        let mut convert_offset = |offset: Box<ast::Expr>| -> Result<usize> {
            let offset = self
                .bind_window_frame_bound_offset(*offset, DataType::Int64)?
                .into_int64();
            if offset < 0 {
                return Err(ErrorCode::InvalidInputSyntax(
                    "offset in window frame bounds must be non-negative".to_owned(),
                )
                .into());
            }
            Ok(offset as usize)
        };
        let mut convert_bound = |bound| -> Result<FrameBound<usize>> {
            Ok(match bound {
                WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                WindowFrameBound::Preceding(Some(offset)) => {
                    FrameBound::Preceding(convert_offset(offset)?)
                }
                WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                WindowFrameBound::Following(Some(offset)) => {
                    FrameBound::Following(convert_offset(offset)?)
                }
            })
        };
        let start = convert_bound(start)?;
        let end = if let Some(end_bound) = end {
            convert_bound(end_bound)?
        } else {
            FrameBound::CurrentRow
        };
        Ok((start, end))
    }

    fn bind_window_frame_scalar_impl_bounds(
        &mut self,
        start: WindowFrameBound,
        end: Option<WindowFrameBound>,
        offset_data_type: &DataType,
    ) -> Result<(FrameBound<ScalarImpl>, FrameBound<ScalarImpl>)> {
        let mut convert_bound = |bound| -> Result<FrameBound<_>> {
            Ok(match bound {
                WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                WindowFrameBound::Preceding(Some(offset)) => FrameBound::Preceding(
                    self.bind_window_frame_bound_offset(*offset, offset_data_type.clone())?,
                ),
                WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                WindowFrameBound::Following(Some(offset)) => FrameBound::Following(
                    self.bind_window_frame_bound_offset(*offset, offset_data_type.clone())?,
                ),
            })
        };
        let start = convert_bound(start)?;
        let end = if let Some(end_bound) = end {
            convert_bound(end_bound)?
        } else {
            FrameBound::CurrentRow
        };
        Ok((start, end))
    }

    fn bind_window_frame_bound_offset(
        &mut self,
        offset: ast::Expr,
        cast_to: DataType,
    ) -> Result<ScalarImpl> {
        let mut offset = self.bind_expr(offset)?;
        if !offset.is_const() {
            return Err(ErrorCode::InvalidInputSyntax(
                "offset/gap in window frame bounds must be constant".to_owned(),
            )
            .into());
        }
        if offset.cast_implicit_mut(cast_to.clone()).is_err() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "offset/gap in window frame bounds must be castable to {}",
                cast_to
            ))
            .into());
        }
        let offset = offset.fold_const()?;
        let Some(offset) = offset else {
            return Err(ErrorCode::InvalidInputSyntax(
                "offset/gap in window frame bounds must not be NULL".to_owned(),
            )
            .into());
        };
        Ok(offset)
    }
}
