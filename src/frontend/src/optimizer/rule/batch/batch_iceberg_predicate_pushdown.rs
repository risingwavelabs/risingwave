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
use iceberg::expr::{Predicate as IcebergPredicate, Reference};
use iceberg::spec::Datum as IcebergDatum;
use risingwave_common::catalog::Field;
use risingwave_common::types::ScalarImpl;

use crate::expr::{Expr, ExprImpl, ExprType, Literal};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{BatchFilter, BatchIcebergScan, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::Condition;

/// NOTE(kwannoel): We do predicate pushdown to the iceberg-sdk here.
/// zone-map is used to evaluate predicates on iceberg tables.
/// Without zone-map, iceberg-sdk will still apply the predicate on its own.
/// See: <https://github.com/apache/iceberg-rust/blob/5c1a9e68da346819072a15327080a498ad91c488/crates/iceberg/src/arrow/reader.rs#L229-L235>.
pub struct BatchIcebergPredicatePushDownRule {}

impl Rule for BatchIcebergPredicatePushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &BatchFilter = plan.as_batch_filter()?;
        let input = filter.input();
        let scan: &BatchIcebergScan = input.as_batch_iceberg_scan()?;
        // NOTE(kwannoel): We only fill iceberg predicate here.
        assert_eq!(scan.predicate, IcebergPredicate::AlwaysTrue);

        let predicate = filter.predicate().clone();
        let (iceberg_predicate, rw_predicate) =
            rw_predicate_to_iceberg_predicate(predicate, scan.schema().fields());
        let scan = scan.clone_with_predicate(iceberg_predicate);
        if rw_predicate.always_true() {
            Some(scan.into())
        } else {
            let filter = filter
                .clone_with_input(scan.into())
                .clone_with_predicate(rw_predicate);
            Some(filter.into())
        }
    }
}

fn rw_literal_to_iceberg_datum(literal: &Literal) -> Option<IcebergDatum> {
    let Some(scalar) = literal.get_data() else {
        return None;
    };
    match scalar {
        ScalarImpl::Bool(b) => Some(IcebergDatum::bool(*b)),
        ScalarImpl::Int32(i) => Some(IcebergDatum::int(*i)),
        ScalarImpl::Int64(i) => Some(IcebergDatum::long(*i)),
        ScalarImpl::Float32(f) => Some(IcebergDatum::float(*f)),
        ScalarImpl::Float64(f) => Some(IcebergDatum::double(*f)),
        ScalarImpl::Decimal(_) => {
            // TODO(iceberg): iceberg-rust doesn't support decimal predicate pushdown yet.
            None
        }
        ScalarImpl::Date(d) => {
            let Ok(datum) = IcebergDatum::date_from_ymd(d.0.year(), d.0.month(), d.0.day()) else {
                return None;
            };
            Some(datum)
        }
        ScalarImpl::Timestamp(t) => Some(IcebergDatum::timestamp_micros(
            t.0.and_utc().timestamp_micros(),
        )),
        ScalarImpl::Timestamptz(t) => Some(IcebergDatum::timestamptz_micros(t.timestamp_micros())),
        ScalarImpl::Utf8(s) => Some(IcebergDatum::string(s)),
        ScalarImpl::Bytea(b) => Some(IcebergDatum::binary(b.clone())),
        _ => None,
    }
}

fn rw_expr_to_iceberg_predicate(expr: &ExprImpl, fields: &[Field]) -> Option<IcebergPredicate> {
    match expr {
        ExprImpl::Literal(l) => match l.get_data() {
            Some(ScalarImpl::Bool(b)) => {
                if *b {
                    Some(IcebergPredicate::AlwaysTrue)
                } else {
                    Some(IcebergPredicate::AlwaysFalse)
                }
            }
            _ => None,
        },
        ExprImpl::FunctionCall(f) => {
            let args = f.inputs();
            match f.func_type() {
                ExprType::Not => {
                    let arg = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                    Some(IcebergPredicate::negate(arg))
                }
                ExprType::And => {
                    let arg0 = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                    let arg1 = rw_expr_to_iceberg_predicate(&args[1], fields)?;
                    Some(IcebergPredicate::and(arg0, arg1))
                }
                ExprType::Or => {
                    let arg0 = rw_expr_to_iceberg_predicate(&args[0], fields)?;
                    let arg1 = rw_expr_to_iceberg_predicate(&args[1], fields)?;
                    Some(IcebergPredicate::or(arg0, arg1))
                }
                ExprType::Equal if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                        | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.equal_to(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::NotEqual if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)]
                        | [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.not_equal_to(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::GreaterThan if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.greater_than(datum))
                        }
                        [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.less_than_or_equal_to(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::GreaterThanOrEqual if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.greater_than_or_equal_to(datum))
                        }
                        [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.less_than(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::LessThan if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.less_than(datum))
                        }
                        [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.greater_than_or_equal_to(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::LessThanOrEqual if args[0].return_type() == args[1].return_type() => {
                    match [&args[0], &args[1]] {
                        [ExprImpl::InputRef(lhs), ExprImpl::Literal(rhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.less_than_or_equal_to(datum))
                        }
                        [ExprImpl::Literal(rhs), ExprImpl::InputRef(lhs)] => {
                            let column_name = &fields[lhs.index].name;
                            let reference = Reference::new(column_name);
                            let datum = rw_literal_to_iceberg_datum(rhs)?;
                            Some(reference.greater_than(datum))
                        }
                        _ => None,
                    }
                }
                ExprType::IsNull => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let column_name = &fields[lhs.index].name;
                        let reference = Reference::new(column_name);
                        Some(reference.is_null())
                    }
                    _ => None,
                },
                ExprType::IsNotNull => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let column_name = &fields[lhs.index].name;
                        let reference = Reference::new(column_name);
                        Some(reference.is_not_null())
                    }
                    _ => None,
                },
                ExprType::In => match &args[0] {
                    ExprImpl::InputRef(lhs) => {
                        let column_name = &fields[lhs.index].name;
                        let reference = Reference::new(column_name);
                        let mut datums = Vec::with_capacity(args.len() - 1);
                        for arg in &args[1..] {
                            if args[0].return_type() != arg.return_type() {
                                return None;
                            }
                            if let ExprImpl::Literal(l) = arg {
                                if let Some(datum) = rw_literal_to_iceberg_datum(l) {
                                    datums.push(datum);
                                } else {
                                    return None;
                                }
                            } else {
                                return None;
                            }
                        }
                        Some(reference.is_in(datums))
                    }
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}
fn rw_predicate_to_iceberg_predicate(
    predicate: Condition,
    fields: &[Field],
) -> (IcebergPredicate, Condition) {
    if predicate.always_true() {
        return (IcebergPredicate::AlwaysTrue, predicate);
    }

    let mut conjunctions = predicate.conjunctions;
    let mut ignored_conjunctions: Vec<ExprImpl> = Vec::with_capacity(conjunctions.len());

    let mut iceberg_condition_root = None;
    while let Some(conjunction) = conjunctions.pop() {
        match rw_expr_to_iceberg_predicate(&conjunction, fields) {
            iceberg_predicate @ Some(_) => {
                iceberg_condition_root = iceberg_predicate;
                break;
            }
            None => {
                ignored_conjunctions.push(conjunction);
                continue;
            }
        }
    }

    let mut iceberg_condition_root = match iceberg_condition_root {
        Some(p) => p,
        None => {
            return (
                IcebergPredicate::AlwaysTrue,
                Condition {
                    conjunctions: ignored_conjunctions,
                },
            );
        }
    };

    for rw_condition in conjunctions {
        match rw_expr_to_iceberg_predicate(&rw_condition, fields) {
            Some(iceberg_predicate) => {
                iceberg_condition_root = iceberg_condition_root.and(iceberg_predicate)
            }
            None => ignored_conjunctions.push(rw_condition),
        }
    }
    (
        iceberg_condition_root,
        Condition {
            conjunctions: ignored_conjunctions,
        },
    )
}

impl BatchIcebergPredicatePushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchIcebergPredicatePushDownRule {})
    }
}
