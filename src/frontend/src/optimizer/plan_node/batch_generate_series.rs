// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::GenerateTimeSeriesNode;

use super::{
    LogicalGenerateSeries, PlanBase, PlanRef, PlanTreeNodeLeaf, ToBatchProst, ToDistributedBatch,
};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone)]
pub struct BatchGenerateSeries {
    pub base: PlanBase,
    logical: LogicalGenerateSeries,
}

impl PlanTreeNodeLeaf for BatchGenerateSeries {}
impl_plan_tree_node_for_leaf!(BatchGenerateSeries);

impl BatchGenerateSeries {
    pub fn new(logical: LogicalGenerateSeries) -> Self {
        Self::with_dist(logical, Distribution::Any)
    }

    pub fn with_dist(logical: LogicalGenerateSeries, dist: Distribution) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any().clone());
        BatchGenerateSeries { base, logical }
    }

    /// Get a reference to the batch GenerateSeries's logical.
    #[must_use]
    pub fn logical(&self) -> &LogicalGenerateSeries {
        &self.logical
    }
}

impl fmt::Display for BatchGenerateSeries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchGenerateSeries")
    }
}

impl ToDistributedBatch for BatchGenerateSeries {
    fn to_distributed(&self) -> PlanRef {
        Self::with_dist(self.logical().clone(), Distribution::Single).into()
    }
}

impl ToBatchProst for BatchGenerateSeries {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::GenerateTimeSeries(GenerateTimeSeriesNode {
            start: Some(self.logical.start.to_protobuf()),
            stop: Some(self.logical.stop.to_protobuf()),
            step: Some(self.logical.step.into()),
        })
    }
}

#[cfg(test)]
mod tests {

    use risingwave_pb::batch_plan::plan_node::NodeBody;
    use risingwave_pb::batch_plan::values_node::ExprTuple;
    use risingwave_pb::batch_plan::ValuesNode;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::{ConstantValue, ExprNode};
    use risingwave_pb::plan_common::Field;

    use crate::expr::ExprType;
    use crate::test_utils::LocalFrontend;
    use crate::FrontendOpts;

    #[tokio::test]
    async fn test_generate_series_to_prost() {
        let frontend = LocalFrontend::new(FrontendOpts::default()).await;
        // Values(1:I32)
        let plan = frontend
            .to_batch_plan("values(1)")
            .await
            .unwrap()
            .to_batch_prost();
        assert_eq!(
            plan.get_node_body().unwrap().clone(),
            NodeBody::Values(ValuesNode {
                tuples: vec![ExprTuple {
                    cells: vec![ExprNode {
                        expr_type: ExprType::ConstantValue as i32,
                        return_type: Some(DataType {
                            type_name: TypeName::Int32 as i32,
                            is_nullable: true,
                            ..Default::default()
                        }),
                        rex_node: Some(RexNode::Constant(ConstantValue {
                            body: 1_i32.to_be_bytes().to_vec()
                        }))
                    }]
                }],
                fields: vec![Field {
                    data_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        is_nullable: true,
                        ..Default::default()
                    }),
                    name: "".to_string(),
                }],
            })
        );
    }
}

impl ToLocalBatch for BatchGenerateSeries {
    fn to_local(&self) -> PlanRef {
        Self::with_dist(self.logical().clone(), Distribution::Single).into()
    }
}
