// Copyright 2023 RisingWave Labs
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

use super::prelude::*;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::plan_node::{BatchIcebergScan, BatchLimit, BatchSeqScan, PlanTreeNodeUnary};

pub struct BatchPushLimitToScanRule {}

impl Rule<Batch> for BatchPushLimitToScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let limit: &BatchLimit = plan.as_batch_limit()?;
        let limit_input = limit.input();

        let pushed_limit = limit.limit() + limit.offset();
        if let Some(scan) = limit_input.as_batch_seq_scan() {
            if scan.limit().is_some() {
                return None;
            }
            let new_scan = BatchSeqScan::new_with_dist(
                scan.core().clone(),
                scan.base.distribution().clone(),
                scan.scan_ranges().iter().cloned().collect_vec(),
                Some(pushed_limit),
            );
            return Some(limit.clone_with_input(new_scan.into()).into());
        }

        let scan: &BatchIcebergScan = limit_input.as_batch_iceberg_scan()?;
        if scan.limit().is_some() {
            return None;
        }
        let new_scan = scan.clone_with_limit(Some(pushed_limit));
        Some(limit.clone_with_input(new_scan.into()).into())
    }
}

impl BatchPushLimitToScanRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchPushLimitToScanRule {})
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Field};
    use risingwave_common::types::DataType;
    use risingwave_connector::source::iceberg::IcebergFileScanTask;

    use super::*;
    use crate::OptimizerContext;
    use crate::optimizer::plan_node::generic::{Limit, Source, SourceNodeKind};

    fn limit_over_iceberg_scan(limit: u64, offset: u64) -> PlanRef {
        let column = ColumnCatalog {
            column_desc: ColumnDesc::from_field_with_column_id(
                &Field {
                    name: "v".to_owned(),
                    data_type: DataType::Int32,
                },
                0,
            ),
            is_hidden: false,
        };
        let source = Source {
            catalog: None,
            column_catalog: vec![column],
            row_id_index: None,
            kind: SourceNodeKind::CreateMViewOrBatch,
            ctx: OptimizerContext::mock(),
            as_of: None,
        };
        let scan = BatchIcebergScan::new(source, IcebergFileScanTask::Data(vec![]));
        BatchLimit::new(Limit::new(scan.into(), limit, offset)).into()
    }

    #[test]
    fn pushes_small_limit_to_iceberg_scan() {
        let plan = limit_over_iceberg_scan(1, 2);
        let rewritten = BatchPushLimitToScanRule {}.apply(plan).unwrap();
        let limit = rewritten.as_batch_limit().unwrap();
        let input = limit.input();
        let scan = input.as_batch_iceberg_scan().unwrap();

        assert_eq!(scan.limit(), Some(3));
    }

    #[test]
    fn pushes_large_limit_to_iceberg_scan() {
        let plan = limit_over_iceberg_scan(10_000, 0);
        let rewritten = BatchPushLimitToScanRule {}.apply(plan).unwrap();
        let limit = rewritten.as_batch_limit().unwrap();
        let input = limit.input();
        let scan = input.as_batch_iceberg_scan().unwrap();

        assert_eq!(scan.limit(), Some(10_000));
    }
}
