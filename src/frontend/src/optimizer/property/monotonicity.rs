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

use enum_as_inner::EnumAsInner;
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type as ExprType;

use crate::expr::{Expr, ExprImpl, FunctionCall, TableFunction};

/// Represents the monotonicity of a column. This enum aims to unify the "non-decreasing analysis" and watermark derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumAsInner)]
pub enum Monotonicity {
    /// The column is constant.
    Constant,
    /// The column is strictly non-decreasing. Basically this is similar to `WATERMARK FOR col AS col`, but the `Watermark`
    /// messages are not supposed to be produced by `WatermarkFilter`.
    Increasing,
    /// The column is strictly non-increasing.
    Decreasing,
    /// The column is GENERALLY non-decreasing, but not STRICTLY. Watermarks should be produced by `WatermarkFilter` and
    /// forwarded to downstream with necessary transformation in other operators like `Project(Set)`.
    /// This is not used currently.
    _IncreasingByWatermark,
    /// The monotonicity of the column follows the monotonicity of the specified column in the input.
    FollowingInput(usize),
    /// The monotonicity of the column INVERSELY follows the monotonicity of the specified column in the input.
    FollowingInputInversely(usize),
    /// The monotonicity of the column is unknown, meaning that we have to do everything conservatively for this column.
    Unknown,
}

impl Monotonicity {
    fn inverse(self) -> Self {
        use Monotonicity::*;
        match self {
            Constant => Constant,
            Increasing => Decreasing,
            Decreasing => Increasing,
            _IncreasingByWatermark => Unknown,
            FollowingInput(idx) => FollowingInputInversely(idx),
            FollowingInputInversely(idx) => FollowingInput(idx),
            Unknown => Unknown,
        }
    }
}

/// Analyze the monotonicity of an expression.
pub fn analyze_monotonicity(expr: &ExprImpl) -> Monotonicity {
    let analyzer = MonotonicityAnalyzer {};
    analyzer.visit_expr(expr)
}

struct MonotonicityAnalyzer {}

impl MonotonicityAnalyzer {
    fn visit_expr(&self, expr: &ExprImpl) -> Monotonicity {
        use Monotonicity::*;
        match expr {
            // recursion base
            ExprImpl::InputRef(inner) => FollowingInput(inner.index()),
            ExprImpl::Literal(_) => Constant,
            ExprImpl::Now(_) => Increasing,
            ExprImpl::UserDefinedFunction(_) => Unknown,

            // recursively visit children
            ExprImpl::FunctionCall(inner) => self.visit_function_call(inner),
            ExprImpl::FunctionCallWithLambda(inner) => self.visit_function_call(inner.base()),
            ExprImpl::TableFunction(inner) => self.visit_table_function(inner),

            // the analyzer is not expected to be used when the following expression types are present
            ExprImpl::Subquery(_)
            | ExprImpl::AggCall(_)
            | ExprImpl::CorrelatedInputRef(_)
            | ExprImpl::WindowFunction(_)
            | ExprImpl::Parameter(_) => panic!(
                "Expression `{}` is not expected in the monotonicity analyzer",
                expr.variant_name()
            ),
        }
    }

    fn visit_unary_op(&self, inputs: &[ExprImpl]) -> Monotonicity {
        assert_eq!(inputs.len(), 1);
        self.visit_expr(&inputs[0])
    }

    fn visit_binary_op(&self, inputs: &[ExprImpl]) -> (Monotonicity, Monotonicity) {
        assert_eq!(inputs.len(), 2);
        (self.visit_expr(&inputs[0]), self.visit_expr(&inputs[1]))
    }

    fn visit_ternary_op(&self, inputs: &[ExprImpl]) -> (Monotonicity, Monotonicity, Monotonicity) {
        assert_eq!(inputs.len(), 3);
        (
            self.visit_expr(&inputs[0]),
            self.visit_expr(&inputs[1]),
            self.visit_expr(&inputs[2]),
        )
    }

    fn visit_function_call(&self, func_call: &FunctionCall) -> Monotonicity {
        use Monotonicity::*;

        fn time_zone_is_without_dst(time_zone: Option<&str>) -> bool {
            let tz_is_utc = time_zone.map_or(
                false, // conservative
                |time_zone| time_zone.eq_ignore_ascii_case("UTC"),
            );
            tz_is_utc // conservative
        }

        match func_call.func_type() {
            ExprType::Unspecified => unreachable!(),
            ExprType::Add => match self.visit_binary_op(func_call.inputs()) {
                (Constant, any) | (any, Constant) => any,
                (Increasing, Increasing) => Increasing,
                (Decreasing, Decreasing) => Decreasing,
                _ => Unknown,
            },
            ExprType::Subtract => match self.visit_binary_op(func_call.inputs()) {
                (any, Constant) => any,
                (Constant, any) => any.inverse(),
                _ => Unknown,
            },
            ExprType::Multiply | ExprType::Divide | ExprType::Modulus => {
                match self.visit_binary_op(func_call.inputs()) {
                    (Constant, Constant) => Constant,
                    _ => Unknown, // let's be lazy here
                }
            }
            ExprType::TumbleStart => {
                if func_call.inputs().len() == 2 {
                    // without `offset`, args: `(start, interval)`
                    match self.visit_binary_op(func_call.inputs()) {
                        (any, Constant) => any,
                        _ => Unknown,
                    }
                } else {
                    // with `offset`, args: `(start, interval, offset)`
                    assert_eq!(ExprType::TumbleStart, func_call.func_type());
                    match self.visit_ternary_op(func_call.inputs()) {
                        (any, Constant, Constant) => any,
                        _ => Unknown,
                    }
                }
            }
            ExprType::AtTimeZone => match self.visit_binary_op(func_call.inputs()) {
                (Constant, Constant) => Constant,
                (any, Constant) => {
                    let time_zone = func_call.inputs()[1]
                        .as_literal()
                        .and_then(|literal| literal.get_data().as_ref())
                        .map(|tz| tz.as_utf8().as_ref());
                    // 1. For at_time_zone(timestamp, const timezone) -> timestamptz, when timestamp has some monotonicity,
                    // the result should have the same monotonicity.
                    // 2. For at_time_zone(timestamptz, const timezone) -> timestamp, when timestamptz has some monotonicity,
                    // the result only have the same monotonicity when the timezone is without DST (Daylight Saving Time).
                    if (func_call.inputs()[0].return_type() == DataType::Timestamp
                        && func_call.return_type() == DataType::Timestamptz)
                        || time_zone_is_without_dst(time_zone)
                    {
                        any
                    } else {
                        Unknown
                    }
                }
                _ => Unknown,
            },
            ExprType::DateTrunc => match func_call.inputs().len() {
                2 => match self.visit_binary_op(func_call.inputs()) {
                    (Constant, any) => any,
                    _ => Unknown,
                },
                3 => match self.visit_ternary_op(func_call.inputs()) {
                    (Constant, Constant, Constant) => Constant,
                    (Constant, any, Constant) => {
                        let time_zone = func_call.inputs()[2]
                            .as_literal()
                            .and_then(|literal| literal.get_data().as_ref())
                            .map(|tz| tz.as_utf8().as_ref());
                        if time_zone_is_without_dst(time_zone) {
                            any
                        } else {
                            Unknown
                        }
                    }
                    _ => Unknown,
                },
                _ => unreachable!(),
            },
            ExprType::AddWithTimeZone | ExprType::SubtractWithTimeZone => {
                // Requires time zone and interval to be literal, at least for now.
                let time_zone = match &func_call.inputs()[2] {
                    ExprImpl::Literal(lit) => {
                        lit.get_data().as_ref().map(|tz| tz.as_utf8().as_ref())
                    }
                    _ => return Unknown,
                };
                let interval = match &func_call.inputs()[1] {
                    ExprImpl::Literal(lit) => lit
                        .get_data()
                        .as_ref()
                        .map(|interval| interval.as_interval()),
                    _ => return Unknown,
                };
                let quantitative_only = interval.map_or(
                    true, // null interval is treated as `interval '1' second`
                    |v| v.months() == 0 && (v.days() == 0 || time_zone_is_without_dst(time_zone)),
                );
                match (self.visit_expr(&func_call.inputs()[0]), quantitative_only) {
                    (Constant, _) => Constant,
                    (any, true) => any,
                    _ => Unknown,
                }
            }
            ExprType::SecToTimestamptz => self.visit_unary_op(func_call.inputs()),
            ExprType::CharToTimestamptz => Unknown,
            ExprType::Cast => {
                // TODO: need more derivation
                Unknown
            }
            ExprType::Case => {
                // TODO: do we need derive watermark when every case can derive a common watermark?
                Unknown
            }
            ExprType::Proctime => Increasing,
            _ => Unknown,
        }
    }

    fn visit_table_function(&self, _table_func: &TableFunction) -> Monotonicity {
        // TODO: derive monotonicity for table funcs like `generate_series`
        Monotonicity::Unknown
    }
}
