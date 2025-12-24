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
use risingwave_common::catalog::{
    Field, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME,
    ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::iceberg::{IcebergFileScanTask, IcebergTaskParameters};
use risingwave_connector::source::prelude::IcebergSplitEnumerator;
use risingwave_connector::source::{ConnectorProperties, SourceEnumeratorContext};

use super::prelude::*;
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::utils::to_iceberg_time_travel_as_of;
use crate::optimizer::plan_node::{LogicalFilter, LogicalIcebergScan, LogicalJoin, LogicalSource};
use crate::optimizer::rule::{ApplyResult, FallibleRule};
use crate::utils::{Condition, FRONTEND_RUNTIME};

pub struct PopulateIcebergTaskAndTransformDeleteRule {}

impl FallibleRule<Logical> for PopulateIcebergTaskAndTransformDeleteRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let scan: &LogicalIcebergScan = plan.as_logical_iceberg_scan()?;
        // 1. Get predicate from LogicalFilter
        let predicate = scan.predicate();
        let schema = scan.schema();
        let (iceberg_predicate, rw_predicate) =
            rw_predicate_to_iceberg_predicate(predicate, scan.schema().fields());
        // 2. Create IcebergTask from LogicalIcebergScan and predicate
        let timezone = plan.ctx().get_session_timezone();
        let as_of = scan.logical_source.core.as_of.clone();
        let time_travel_info = to_iceberg_time_travel_as_of(&as_of, &timezone)?;

        let s = if let ConnectorProperties::Iceberg(prop) = ConnectorProperties::extract(
            scan.logical_source
                .core
                .catalog
                .as_ref()
                .unwrap()
                .with_properties
                .clone(),
            false,
        )? {
            IcebergSplitEnumerator::new_inner(*prop, SourceEnumeratorContext::dummy().into())
        } else {
            return ApplyResult::Err(
                crate::error::ErrorCode::BindError(
                    "iceberg_scan can't be used in the madsim mode".to_string(),
                )
                .into(),
            );
        };

        let delete_parameters: IcebergTaskParameters = tokio::task::block_in_place(|| {
            FRONTEND_RUNTIME.block_on(s.get_iceberg_task_parameters(
                schema.clone(),
                iceberg_predicate,
                time_travel_info,
            ))
        })?;
        // 3. Transform delete scan to data scan hash join delete scan
        let IcebergTaskParameters {
            data_tasks,
            equality_delete_tasks,
            equality_delete_columns,
            position_delete_tasks,
            count,
            snapshot_id,
        } = delete_parameters;
        let mut iceberg_scan: PlanRef = scan
            .clone_with_iceberg_file_scan_task(data_tasks, count)
            .into();

        if !equality_delete_columns.is_empty() {
            iceberg_scan = build_equality_delete_hashjoin_scan(
                &scan.logical_source,
                equality_delete_columns,
                equality_delete_tasks,
                iceberg_scan,
                snapshot_id,
            )?;
        }
        if !position_delete_tasks.is_empty() {
            iceberg_scan = build_position_delete_hashjoin_scan(
                &scan.logical_source,
                iceberg_scan,
                position_delete_tasks,
                snapshot_id,
            )?;
        }

        if rw_predicate.always_true() {
            ApplyResult::Ok(iceberg_scan.into())
        } else {
            let filter = LogicalFilter::new(iceberg_scan, rw_predicate);
            ApplyResult::Ok(filter.into())
        }
    }
}

impl PopulateIcebergTaskAndTransformDeleteRule {
    pub fn create() -> BoxedRule {
        Box::new(PopulateIcebergTaskAndTransformDeleteRule {})
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

fn build_equality_delete_hashjoin_scan(
    source: &LogicalSource,
    delete_column_names: Vec<String>,
    equality_delete_tasks: IcebergFileScanTask,
    data_iceberg_scan: PlanRef,
    snapshot_id: Option<i64>,
) -> Result<PlanRef> {
    // equality delete scan
    let column_catalog_map = source
        .core
        .column_catalog
        .iter()
        .map(|c| (&c.column_desc.name, c))
        .collect::<std::collections::HashMap<_, _>>();
    let column_catalog: Vec<_> = delete_column_names
        .iter()
        .chain(std::iter::once(
            &ICEBERG_SEQUENCE_NUM_COLUMN_NAME.to_owned(),
        ))
        .map(|name| *column_catalog_map.get(&name).unwrap())
        .cloned()
        .collect();
    let equality_delete_source = source.clone_with_column_catalog(column_catalog)?;
    let equality_delete_iceberg_scan = LogicalIcebergScan::new_with_iceberg_file_scan_task(
        &equality_delete_source,
        equality_delete_tasks,
        0,
        snapshot_id,
    );

    let data_columns_len = data_iceberg_scan.schema().len();
    // The join condition is delete_column_names is equal and sequence number is less than, join type is left anti
    let build_inputs = |scan: &PlanRef, offset: usize| {
        let delete_column_index_map = scan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, data_column)| (&data_column.name, (index, &data_column.data_type)))
            .collect::<std::collections::HashMap<_, _>>();
        let delete_column_inputs = delete_column_names
            .iter()
            .map(|name| {
                let (index, data_type) = delete_column_index_map.get(name).unwrap();
                InputRef {
                    index: offset + index,
                    data_type: (*data_type).clone(),
                }
            })
            .collect::<Vec<InputRef>>();
        let seq_num_inputs = InputRef {
            index: scan
                .schema()
                .fields()
                .iter()
                .position(|f| f.name.eq(ICEBERG_SEQUENCE_NUM_COLUMN_NAME))
                .unwrap()
                + offset,
            data_type: risingwave_common::types::DataType::Int64,
        };
        (delete_column_inputs, seq_num_inputs)
    };
    let (join_left_delete_column_inputs, join_left_seq_num_input) =
        build_inputs(&data_iceberg_scan, 0);
    let equality_delete_iceberg_scan = equality_delete_iceberg_scan.into();
    let (join_right_delete_column_inputs, join_right_seq_num_input) =
        build_inputs(&equality_delete_iceberg_scan, data_columns_len);

    let mut eq_join_expr = join_left_delete_column_inputs
        .iter()
        .zip_eq_fast(join_right_delete_column_inputs.iter())
        .map(|(left, right)| {
            Ok(FunctionCall::new(
                ExprType::Equal,
                vec![left.clone().into(), right.clone().into()],
            )?
            .into())
        })
        .collect::<Result<Vec<ExprImpl>>>()?;
    eq_join_expr.push(
        FunctionCall::new(
            ExprType::LessThan,
            vec![
                join_left_seq_num_input.into(),
                join_right_seq_num_input.into(),
            ],
        )?
        .into(),
    );
    let on = Condition {
        conjunctions: eq_join_expr,
    };
    let join = LogicalJoin::new(
        data_iceberg_scan,
        equality_delete_iceberg_scan,
        risingwave_pb::plan_common::JoinType::LeftAnti,
        on,
    );
    Ok(join.into())
}

fn build_position_delete_hashjoin_scan(
    source: &LogicalSource,
    data_iceberg_scan: PlanRef,
    position_delete_tasks: IcebergFileScanTask,
    snapshot_id: Option<i64>,
) -> Result<PlanRef> {
    // FILE_PATH, FILE_POS
    let column_catalog = source
        .core
        .column_catalog
        .iter()
        .filter(|c| {
            c.column_desc.name.eq(ICEBERG_FILE_PATH_COLUMN_NAME)
                || c.column_desc.name.eq(ICEBERG_FILE_POS_COLUMN_NAME)
        })
        .cloned()
        .collect();
    let position_delete_source = source.clone_with_column_catalog(column_catalog)?;
    let position_delete_iceberg_scan = LogicalIcebergScan::new_with_iceberg_file_scan_task(
        &position_delete_source,
        position_delete_tasks,
        0,
        snapshot_id,
    );
    let data_columns_len = data_iceberg_scan.schema().len();

    let build_inputs = |scan: &PlanRef, offset: usize| {
        scan.schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, data_column)| {
                if data_column.name.eq(ICEBERG_FILE_PATH_COLUMN_NAME)
                    || data_column.name.eq(ICEBERG_FILE_POS_COLUMN_NAME)
                {
                    Some(InputRef {
                        index: offset + index,
                        data_type: data_column.data_type(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<InputRef>>()
    };
    let join_left_delete_column_inputs = build_inputs(&data_iceberg_scan, 0);
    let position_delete_iceberg_scan = position_delete_iceberg_scan.into();
    let join_right_delete_column_inputs =
        build_inputs(&position_delete_iceberg_scan, data_columns_len);
    let eq_join_expr = join_left_delete_column_inputs
        .iter()
        .zip_eq_fast(join_right_delete_column_inputs.iter())
        .map(|(left, right)| {
            Ok(FunctionCall::new(
                ExprType::Equal,
                vec![left.clone().into(), right.clone().into()],
            )?
            .into())
        })
        .collect::<Result<Vec<ExprImpl>>>()?;
    let on = Condition {
        conjunctions: eq_join_expr,
    };
    let join = LogicalJoin::new(
        data_iceberg_scan,
        position_delete_iceberg_scan,
        risingwave_pb::plan_common::JoinType::LeftAnti,
        on,
    );
    Ok(join.into())
}
