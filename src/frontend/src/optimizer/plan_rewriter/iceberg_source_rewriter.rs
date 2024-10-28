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

use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::plan_node::{LogicalIcebergScan, LogicalJoin, LogicalSource};
use crate::optimizer::PlanRef;
use crate::utils::{Condition, FRONTEND_RUNTIME};

#[derive(Debug, Clone, Default)]
pub struct IcebergSourceRewriter {}

impl IcebergSourceRewriter {
    pub fn rewrite(plan: &PlanRef) -> Result<PlanRef> {
        Self::rewrite_logical_source(plan)
    }

    fn rewrite_logical_source(plan: &PlanRef) -> Result<PlanRef> {
        let source: &LogicalSource = match plan.as_logical_source() {
            Some(s) => s,
            None => return Ok(plan.clone()),
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
                return Ok(plan.clone());
            };
            let delete_column_names = std::thread::spawn(move || {
                FRONTEND_RUNTIME.block_on(s.get_all_delete_column_names())
            })
            .join()
            .unwrap()?;
            // data file scan
            let data_iceberg_scan = LogicalIcebergScan::new(source, IcebergScanType::DataScan);
            // equality delete scan
            let delete_iceberg_scan =
                LogicalIcebergScan::new(source, IcebergScanType::EqualityDeleteScan);
            let data_columns_len = data_iceberg_scan.core.schema().len();
            // The join condition is delete_column_names is equal and sequence number is less than, join type is left anti
            let eq_join_expr = data_iceberg_scan
                .core
                .schema()
                .fields()
                .iter()
                .zip_eq_fast(delete_iceberg_scan.core.schema().fields().iter())
                .enumerate()
                .filter_map(|(index, (data_column, delete_column))| {
                    if delete_column_names.contains(&data_column.name) {
                        let data_input_ref = InputRef {
                            index,
                            data_type: data_column.data_type(),
                        };
                        let delete_input_ref = InputRef {
                            index: index + data_columns_len,
                            data_type: delete_column.data_type(),
                        };
                        Some(
                            FunctionCall::new(
                                ExprType::Equal,
                                vec![data_input_ref.into(), delete_input_ref.into()],
                            )
                            .unwrap()
                            .into(),
                        )
                    } else if data_column.name.eq(ICEBERG_SEQUENCE_NUM_COLUMN_NAME) {
                        let data_input_ref = InputRef {
                            index,
                            data_type: data_column.data_type(),
                        };
                        let delete_input_ref = InputRef {
                            index: index + data_columns_len,
                            data_type: delete_column.data_type(),
                        };
                        Some(
                            FunctionCall::new(
                                ExprType::LessThan,
                                vec![data_input_ref.into(), delete_input_ref.into()],
                            )
                            .unwrap()
                            .into(),
                        )
                    } else {
                        None
                    }
                })
                .collect::<Vec<ExprImpl>>();
            let on = Condition {
                conjunctions: eq_join_expr,
            };
            let join = LogicalJoin::new(
                data_iceberg_scan.into(),
                delete_iceberg_scan.into(),
                risingwave_pb::plan_common::JoinType::LeftAnti,
                on,
            );
            Ok(join.into())
        } else {
            Ok(plan.clone())
        }
    }
}
