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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::{BoxedRule, Rule};
use crate::expr::{Expr, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFileScan, LogicalTableFunction};
use crate::optimizer::PlanRef;

/// Transform a special `TableFunction` (with `FILE_SCAN` table function type) into a `LogicalFileScan`
pub struct TableFunctionToFileScanRule {}
impl Rule for TableFunctionToFileScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type != TableFunctionType::FileScan {
            return None;
        }
        assert!(!logical_table_function.with_ordinality);
        let table_function_return_type = logical_table_function.table_function().return_type();

        if let DataType::Struct(st) = table_function_return_type.clone() {
            let fields = st
                .types()
                .zip_eq_debug(st.names())
                .map(|(data_type, name)| Field::with_name(data_type.clone(), name.to_owned()))
                .collect_vec();

            let schema = Schema::new(fields);

            let mut eval_args = vec![];
            for arg in &logical_table_function.table_function().args {
                assert_eq!(arg.return_type(), DataType::Varchar);
                let value = arg.try_fold_const().unwrap().unwrap();
                match value {
                    Some(ScalarImpl::Utf8(s)) => {
                        eval_args.push(s.to_string());
                    }
                    _ => {
                        unreachable!("must be a varchar")
                    }
                }
            }
            assert!("parquet".eq_ignore_ascii_case(&eval_args[0]));
            assert!(
                ("s3".eq_ignore_ascii_case(&eval_args[1]))
                    || "gcs".eq_ignore_ascii_case(&eval_args[1])
            );

            if "s3".eq_ignore_ascii_case(&eval_args[1]) {
                let s3_region = eval_args[2].clone();
                let s3_access_key = eval_args[3].clone();
                let s3_secret_key = eval_args[4].clone();
                // The rest of the arguments are file locations
                let file_location = eval_args[5..].iter().cloned().collect_vec();
                Some(
                    LogicalFileScan::new_s3_logical_file_scan(
                        logical_table_function.ctx(),
                        schema,
                        "parquet".to_owned(),
                        "s3".to_owned(),
                        s3_region,
                        s3_access_key,
                        s3_secret_key,
                        file_location,
                    )
                    .into(),
                )
            } else if "gcs".eq_ignore_ascii_case(&eval_args[1]) {
                let creditial = eval_args[2].clone();
                let service_account = eval_args[3].clone();
                // The rest of the arguments are file locations
                let file_location = eval_args[4..].iter().cloned().collect_vec();
                Some(
                    LogicalFileScan::new_gcs_logical_file_scan(
                        logical_table_function.ctx(),
                        schema,
                        "parquet".to_owned(),
                        "gcs".to_owned(),
                        creditial,
                        service_account,
                        file_location,
                    )
                    .into(),
                )
            } else {
                unreachable!()
            }
        } else {
            unreachable!("TableFunction return type should be struct")
        }
    }
}

impl TableFunctionToFileScanRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToFileScanRule {})
    }
}
