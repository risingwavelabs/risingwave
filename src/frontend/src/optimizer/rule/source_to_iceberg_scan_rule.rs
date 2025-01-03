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

use risingwave_common::catalog::{
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::iceberg::IcebergSplitEnumerator;
use risingwave_connector::source::{ConnectorProperties, SourceEnumeratorContext};
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::{ApplyResult, BoxedRule, FallibleRule};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalIcebergScan, LogicalJoin, LogicalSource};
use crate::optimizer::PlanRef;
use crate::utils::{Condition, FRONTEND_RUNTIME};

pub struct SourceToIcebergScanRule {}
impl FallibleRule for SourceToIcebergScanRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let source: &LogicalSource = match plan.as_logical_source() {
            Some(s) => s,
            None => return ApplyResult::NotApplicable,
        };
        if source.core.is_iceberg_connector() {
            let s = if let ConnectorProperties::Iceberg(prop) = ConnectorProperties::extract(
                source
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
                return ApplyResult::NotApplicable;
            };

            #[cfg(madsim)]
            return ApplyResult::Err(
                crate::error::ErrorCode::BindError(
                    "iceberg_scan can't be used in the madsim mode".to_string(),
                )
                .into(),
            );
            #[cfg(not(madsim))]
            {
                let (delete_column_names, have_position_delete) =
                    tokio::task::block_in_place(|| {
                        FRONTEND_RUNTIME.block_on(s.get_delete_parameters())
                    })?;
                // data file scan
                let mut data_iceberg_scan: PlanRef =
                    LogicalIcebergScan::new(source, IcebergScanType::DataScan).into();
                if !delete_column_names.is_empty() {
                    data_iceberg_scan = build_equality_delete_hashjoin_scan(
                        source,
                        delete_column_names,
                        data_iceberg_scan,
                    )?;
                }
                if have_position_delete {
                    data_iceberg_scan =
                        build_position_delete_hashjoin_scan(source, data_iceberg_scan)?;
                }
                ApplyResult::Ok(data_iceberg_scan)
            }
        } else {
            ApplyResult::NotApplicable
        }
    }
}

fn build_equality_delete_hashjoin_scan(
    source: &LogicalSource,
    delete_column_names: Vec<String>,
    data_iceberg_scan: PlanRef,
) -> Result<PlanRef> {
    // equality delete scan
    let column_catalog = source
        .core
        .column_catalog
        .iter()
        .filter(|c| {
            delete_column_names.contains(&c.column_desc.name)
                || c.column_desc.name.eq(ICEBERG_SEQUENCE_NUM_COLUMN_NAME)
        })
        .cloned()
        .collect();
    let equality_delete_source = source.clone_with_column_catalog(column_catalog)?;
    let equality_delete_iceberg_scan =
        LogicalIcebergScan::new(&equality_delete_source, IcebergScanType::EqualityDeleteScan);

    let data_columns_len = data_iceberg_scan.schema().len();
    // The join condition is delete_column_names is equal and sequence number is less than, join type is left anti
    let build_inputs = |scan: &PlanRef, offset: usize| {
        let delete_column_inputs = scan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, data_column)| {
                if delete_column_names.contains(&data_column.name) {
                    Some(InputRef {
                        index: offset + index,
                        data_type: data_column.data_type(),
                    })
                } else {
                    None
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
    let position_delete_iceberg_scan =
        LogicalIcebergScan::new(&position_delete_source, IcebergScanType::PositionDeleteScan);
    let data_columns_len = data_iceberg_scan.schema().len();

    let build_inputs = |scan: &PlanRef, offset: usize| {
        let delete_column_inputs = scan
            .schema()
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
            .collect::<Vec<InputRef>>();
        delete_column_inputs
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

impl SourceToIcebergScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergScanRule {})
    }
}
