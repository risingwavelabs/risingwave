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

use risingwave_common::catalog::ICEBERG_SEQUENCE_NUM_COLUMN_NAME;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::iceberg::IcebergSplitEnumerator;
use risingwave_connector::source::{ConnectorProperties, SourceEnumeratorContext};
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::{ApplyResult, BoxedRule, FallibleRule};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanNode;
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
                let delete_column_names = tokio::task::block_in_place(|| {
                    FRONTEND_RUNTIME.block_on(s.get_all_delete_column_names())
                })?;
                // data file scan
                let data_iceberg_scan = LogicalIcebergScan::new(source, IcebergScanType::DataScan);
                if delete_column_names.is_empty() {
                    return ApplyResult::Ok(data_iceberg_scan.into());
                }
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
                let equality_delete_iceberg_scan = LogicalIcebergScan::new(
                    &equality_delete_source,
                    IcebergScanType::EqualityDeleteScan,
                );

                let data_columns_len = data_iceberg_scan.core.schema().len();
                // The join condition is delete_column_names is equal and sequence number is less than, join type is left anti
                let build_inputs = |scan: &LogicalIcebergScan, add_index: usize| {
                    let delete_column_inputs = scan
                        .core
                        .schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .filter_map(|(index, data_column)| {
                            if delete_column_names.contains(&data_column.name) {
                                Some(InputRef {
                                    index: add_index + index,
                                    data_type: data_column.data_type(),
                                })
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<InputRef>>();
                    let seq_num_inputs = InputRef {
                        index: scan
                            .core
                            .schema()
                            .fields()
                            .iter()
                            .position(|f| f.name.eq(ICEBERG_SEQUENCE_NUM_COLUMN_NAME))
                            .unwrap()
                            + add_index,
                        data_type: risingwave_common::types::DataType::Int64,
                    };
                    (delete_column_inputs, seq_num_inputs)
                };
                let (join_left_delete_column_inputs, join_left_seq_num_input) =
                    build_inputs(&data_iceberg_scan, 0);
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
                    data_iceberg_scan.into(),
                    equality_delete_iceberg_scan.into(),
                    risingwave_pb::plan_common::JoinType::LeftAnti,
                    on,
                );
                ApplyResult::Ok(join.into())
            }
        } else {
            ApplyResult::NotApplicable
        }
    }
}

impl SourceToIcebergScanRule {
    pub fn create() -> BoxedRule {
        Box::new(SourceToIcebergScanRule {})
    }
}
