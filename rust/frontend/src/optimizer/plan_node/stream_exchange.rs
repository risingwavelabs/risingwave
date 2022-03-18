use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{DispatchStrategy, DispatcherType, ExchangeNode};

use super::{PlanRef, PlanTreeNodeUnary, StreamBase, ToStreamProst};
use crate::optimizer::property::order::WithOrder;
use crate::optimizer::property::{Distribution, WithDistribution, WithSchema};

/// `StreamExchange` imposes a particular distribution on its input
/// without changing its content.
#[derive(Debug, Clone)]
pub struct StreamExchange {
    pub base: StreamBase,
    input: PlanRef,
    schema: Schema,
}

impl StreamExchange {
    pub fn new(input: PlanRef, dist: Distribution) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let base = StreamBase {
            dist,
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        StreamExchange {
            input,
            schema,
            base,
        }
    }
}

impl fmt::Display for StreamExchange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamExchange")
            .field("order", self.order())
            .field("dist", self.distribution())
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.distribution().clone())
    }
}
impl_plan_tree_node_for_unary! {StreamExchange}

impl WithSchema for StreamExchange {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ToStreamProst for StreamExchange {
    fn to_stream_prost_body(&self) -> Node {
        Node::ExchangeNode(ExchangeNode {
            fields: self.schema.to_prost(),
            strategy: Some(DispatchStrategy {
                r#type: match &self.base.dist {
                    Distribution::HashShard(_) => DispatcherType::Hash,
                    Distribution::Single => DispatcherType::Simple,
                    Distribution::Broadcast => DispatcherType::Broadcast,
                    _ => panic!("Do not allow Any or AnyShard in serialization process"),
                } as i32,
                column_indices: match &self.base.dist {
                    Distribution::HashShard(keys) => keys.iter().map(|num| *num as u32).collect(),
                    _ => vec![],
                },
            }),
        })
    }
}
