//  Copyright 2025 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use chrono::Datelike;
use iceberg::spec::Datum as IcebergDatum;
use risingwave_common::types::ScalarImpl;

use super::prelude::*;
use crate::expr::{Expr, ExprImpl, ExprType, Literal};
use crate::optimizer::plan_node::{LogicalFilter, LogicalIcebergScan, PlanTreeNodeUnary};
use crate::utils::Condition;

/// NOTE(kwannoel): We do predicate pushdown to the iceberg-sdk here.
/// zone-map is used to evaluate predicates on iceberg tables.
/// Without zone-map, iceberg-sdk will still apply the predicate on its own.
/// See: <https://github.com/apache/iceberg-rust/blob/5c1a9e68da346819072a15327080a498ad91c488/crates/iceberg/src/arrow/reader.rs#L229-L235>.
pub struct LogicalIcebergPredicatePushDownRule {}

impl Rule<Logical> for LogicalIcebergPredicatePushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let input = filter.input();
        let scan: &LogicalIcebergScan = input.as_logical_iceberg_scan()?;
        // NOTE(kwannoel): We only fill iceberg predicate here.
        assert!(scan.predicate().always_true());

        let predicate = filter.predicate().clone();
        let (iceberg_predicate, rw_predicate) = rw_predicate_to_pushdown_predicate(predicate);
        // set the scan predicate to the pushdownable part
        let scan = scan.clone_with_predicate(iceberg_predicate);
        if rw_predicate.always_true() {
            Some(scan.into())
        } else {
            let filter = LogicalFilter::create(scan.into(), rw_predicate);
            Some(filter)
        }
    }
}

fn rw_predicate_to_pushdown_predicate(predicate: Condition) -> (Condition, Condition) {
    if predicate.always_true() {
        return (Condition::true_cond(), predicate);
    }

    let mut pushdown_conjs: Vec<ExprImpl> = Vec::new();
    let mut remaining_conjs: Vec<ExprImpl> = Vec::new();

    for conj in predicate.conjunctions {
        if rw_expr_can_convert_to_iceberg_predicate(&conj) {
            pushdown_conjs.push(conj);
        } else {
            remaining_conjs.push(conj);
        }
    }

    (
        Condition {
            conjunctions: pushdown_conjs,
        },
        Condition {
            conjunctions: remaining_conjs,
        },
    )
}

/// Lightweight check whether an expression can be converted into an Iceberg
/// predicate. This mirrors `rw_expr_to_iceberg_predicate` but returns only a
/// boolean and avoids constructing `IcebergPredicate` values.
fn rw_expr_can_convert_to_iceberg_predicate(expr: &ExprImpl) -> bool {
    let datatype_can_pushdown_iceberg = |literal: &Literal| {
        let Some(scalar) = literal.get_data() else {
            return false;
        };
        match scalar {
            ScalarImpl::Bool(_)
            | ScalarImpl::Int32(_)
            | ScalarImpl::Int64(_)
            | ScalarImpl::Float32(_)
            | ScalarImpl::Float64(_)
            | ScalarImpl::Timestamp(_)
            | ScalarImpl::Timestamptz(_)
            | ScalarImpl::Utf8(_)
            | ScalarImpl::Bytea(_) => true,
            ScalarImpl::Decimal(_) => {
                // TODO(iceberg): iceberg-rust doesn't support decimal predicate pushdown yet.
                false
            }
            ScalarImpl::Date(d) => {
                let Ok(_) = IcebergDatum::date_from_ymd(d.0.year(), d.0.month(), d.0.day()) else {
                    return false;
                };
                true
            }
            _ => false,
        }
    };

    match expr {
        ExprImpl::Literal(l) => matches!(l.get_data(), Some(ScalarImpl::Bool(_))),
        ExprImpl::FunctionCall(f) => {
            let args = f.inputs();
            match f.func_type() {
                ExprType::Not => rw_expr_can_convert_to_iceberg_predicate(&args[0]),
                ExprType::And | ExprType::Or => {
                    rw_expr_can_convert_to_iceberg_predicate(&args[0])
                        && rw_expr_can_convert_to_iceberg_predicate(&args[1])
                }
                ExprType::Equal
                | ExprType::NotEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual
                | ExprType::LessThan
                | ExprType::LessThanOrEqual
                    if args[0].return_type() == args[1].return_type() =>
                {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(_), ExprImpl::Literal(rhs)]
                        | [ExprImpl::Literal(rhs), ExprImpl::InputRef(_)] => {
                            datatype_can_pushdown_iceberg(rhs)
                        }
                        _ => false,
                    }
                }
                ExprType::IsNull | ExprType::IsNotNull => matches!(&args[0], ExprImpl::InputRef(_)),
                ExprType::In => match &args[0] {
                    ExprImpl::InputRef(_) => {
                        // All values must be literals of convertible type and have the same return type
                        let first_ret = args[0].return_type();
                        for arg in &args[1..] {
                            if first_ret != arg.return_type() {
                                return false;
                            }
                            if let ExprImpl::Literal(lit) = arg {
                                if !datatype_can_pushdown_iceberg(lit) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                        true
                    }
                    _ => false,
                },
                _ => false,
            }
        }
        _ => false,
    }
}

impl LogicalIcebergPredicatePushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(LogicalIcebergPredicatePushDownRule {})
    }
}
