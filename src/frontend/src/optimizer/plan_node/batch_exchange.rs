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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{ExchangeNode, MergeSortExchangeNode};

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, DistributionDisplay, Order, OrderDisplay};

/// `BatchExchange` imposes a particular distribution on its input
/// without changing its content.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchExchange {
    pub base: PlanBase<Batch>,
    input: PlanRef,
    // sequential means each tasks of the exchange node will be executed sequentially.
    // Currently, it is used to avoid spawn too many tasks for limit operator.
    sequential: bool,
}

impl BatchExchange {
    pub fn new(input: PlanRef, order: Order, dist: Distribution) -> Self {
        Self::new_inner(input, order, dist, false)
    }

    pub fn new_with_sequential(input: PlanRef, order: Order, dist: Distribution) -> Self {
        Self::new_inner(input, order, dist, true)
    }

    fn new_inner(input: PlanRef, order: Order, dist: Distribution, sequential: bool) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let base = PlanBase::new_batch(ctx, schema, dist, order);
        BatchExchange {
            base,
            input,
            sequential,
        }
    }
}

impl Distill for BatchExchange {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let input_schema = self.input.schema();
        let order = OrderDisplay {
            order: self.base.order(),
            input_schema,
        }
        .distill();
        let dist = Pretty::display(&DistributionDisplay {
            distribution: self.base.distribution(),
            input_schema,
        });
        let mut fields = vec![("order", order), ("dist", dist)];
        if self.sequential {
            fields.push(("sequential", Pretty::display(&true)));
        }
        childless_record("BatchExchange", fields)
    }
}

impl PlanTreeNodeUnary for BatchExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new_inner(
            input,
            self.order().clone(),
            self.distribution().clone(),
            self.sequential,
        )
    }
}
impl_plan_tree_node_for_unary! {BatchExchange}

impl ToDistributedBatch for BatchExchange {
    fn to_distributed(&self) -> Result<PlanRef> {
        unreachable!()
    }
}

/// The serialization of Batch Exchange is default cuz it will be rewritten in scheduler.
impl ToBatchPb for BatchExchange {
    fn to_batch_prost_body(&self) -> NodeBody {
        if self.base.order().is_any() {
            NodeBody::Exchange(ExchangeNode {
                sources: vec![],
                sequential: self.sequential,
                input_schema: self.base.schema().to_prost(),
            })
        } else {
            assert!(!self.sequential);
            NodeBody::MergeSortExchange(MergeSortExchangeNode {
                exchange: Some(ExchangeNode {
                    sources: vec![],
                    sequential: self.sequential,
                    input_schema: self.base.schema().to_prost(),
                }),
                column_orders: self.base.order().to_protobuf(),
            })
        }
    }
}

impl ToLocalBatch for BatchExchange {
    fn to_local(&self) -> Result<PlanRef> {
        unreachable!()
    }
}

impl ExprRewritable for BatchExchange {}

impl ExprVisitable for BatchExchange {}
