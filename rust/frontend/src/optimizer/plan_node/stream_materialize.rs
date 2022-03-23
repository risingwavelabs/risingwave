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
use std::collections::HashSet;
use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::ColumnOrder;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;

use super::{PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::plan_node::PlanBase;
use crate::optimizer::property::{FieldOrder, WithSchema};
use crate::session::QueryContextRef;

/// Materializes a stream.
#[derive(Debug, Clone)]
pub struct StreamMaterialize {
    pub base: PlanBase,
    /// Order of columns. We don't use the one in `base` as stream plans generally won't have
    /// `order` property.
    column_orders: Vec<FieldOrder>,

    /// Column Ids
    column_ids: Vec<ColumnId>,

    /// Child of Materialize plan
    input: PlanRef,

    /// Table Id of the materialized table
    table_id: TableId,
}

impl StreamMaterialize {
    pub fn new(
        ctx: QueryContextRef,
        input: PlanRef,
        table_id: TableId,
        mut column_orders: Vec<FieldOrder>,
        column_ids: Vec<ColumnId>,
    ) -> Self {
        let pks = input.pk_indices();
        let ordered_ids: HashSet<usize> = column_orders.iter().map(|x| x.index).collect();

        for pk in pks {
            if !ordered_ids.contains(pk) {
                column_orders.push(FieldOrder::ascending(*pk));
            }
        }

        let base = PlanBase::new_stream(
            ctx,
            input.schema().clone(),
            input.pk_indices().to_vec(),
            input.distribution().clone(),
        );

        Self {
            base,
            input,
            table_id,
            column_orders,
            column_ids,
        }
    }
}

impl fmt::Display for StreamMaterialize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamMaterialize {{ table_id: {}, column_order: {:?}, column_id: {:?}, pk_indices: {:?} }}",
            self.table_id, self.column_orders, self.column_ids, self.base.pk_indices
        )
    }
}

impl PlanTreeNodeUnary for StreamMaterialize {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            self.base.ctx.clone(),
            input,
            self.table_id,
            self.column_orders.clone(),
            self.column_ids.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { StreamMaterialize }

impl WithSchema for StreamMaterialize {
    fn schema(&self) -> &Schema {
        &self.base.schema
    }
}

impl ToStreamProst for StreamMaterialize {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::MaterializeNode(MaterializeNode {
            table_ref_id: None,
            associated_table_ref_id: None,
            column_ids: self.column_ids.iter().map(ColumnId::get_id).collect(),
            column_orders: self
                .column_orders
                .iter()
                .map(|x| {
                    let (input_ref, order_type) = x.to_protobuf();
                    ColumnOrder {
                        input_ref: Some(input_ref),
                        order_type: order_type.into(),
                        return_type: Some(self.schema()[x.index].data_type.to_protobuf()),
                    }
                })
                .collect(),
        })
    }
}
