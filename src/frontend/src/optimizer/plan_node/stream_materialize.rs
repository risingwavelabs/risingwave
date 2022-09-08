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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::FragmentId;
use crate::optimizer::plan_node::{PlanBase, PlanNode};
use crate::optimizer::property::{Direction, Distribution, FieldOrder, Order, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// The first column id to allocate for a new materialized view.
///
/// Note: not starting from 0 helps us to debug misusing of the column id and the index.
const COLUMN_ID_BASE: i32 = 1000;

/// Materializes a stream.
#[derive(Debug, Clone)]
pub struct StreamMaterialize {
    pub base: PlanBase,
    /// Child of Materialize plan
    input: PlanRef,
    table: TableCatalog,
}

impl StreamMaterialize {
    fn derive_plan_base(input: &PlanRef) -> Result<PlanBase> {
        let ctx = input.ctx();

        let schema = input.schema().clone();
        let pk_indices = input.logical_pk();

        // Materialize executor won't change the append-only behavior of the stream, so it depends
        // on input's `append_only`.
        Ok(PlanBase::new_stream(
            ctx,
            schema,
            pk_indices.to_vec(),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
        ))
    }

    #[must_use]
    pub fn new(input: PlanRef, table: TableCatalog) -> Self {
        let base = Self::derive_plan_base(&input).unwrap();
        Self { base, input, table }
    }

    /// Create a materialize node.
    ///
    /// When creating index, `is_index` should be true. Then, materialize will distribute keys
    /// using order by columns, instead of pk.
    pub fn create(
        input: PlanRef,
        mv_name: String,
        user_order_by: Order,
        user_cols: FixedBitSet,
        out_names: Vec<String>,
        is_index_on: Option<TableId>,
    ) -> Result<Self> {
        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => {
                if is_index_on.is_some() {
                    RequiredDist::PhysicalDist(Distribution::HashShard(
                        user_order_by.field_order.iter().map(|x| x.index).collect(),
                    ))
                } else {
                    // ensure the same pk will not shuffle to different node
                    RequiredDist::shard_by_key(input.schema().len(), input.logical_pk())
                }
            }
        };

        let input = required_dist.enforce_if_not_satisfies(input, &Order::any())?;
        let base = Self::derive_plan_base(&input)?;
        let schema = &base.schema;
        let pk_indices = &base.logical_pk;

        let mut col_names = HashSet::new();
        for name in &out_names {
            if !col_names.insert(name.clone()) {
                return Err(
                    InternalError(format!("column {} specified more than once", name)).into(),
                );
            }
        }
        let mut out_name_iter = out_names.into_iter();
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let mut c = ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        field,
                        i as i32 + COLUMN_ID_BASE,
                    ),
                    is_hidden: !user_cols.contains(i),
                };
                c.column_desc.name = if !c.is_hidden {
                    out_name_iter.next().unwrap()
                } else {
                    let mut name = field.name.clone();
                    let mut count = 0;

                    while !col_names.insert(name.clone()) {
                        count += 1;
                        name = field.name.clone() + "#" + &count.to_string();
                    }

                    name
                };
                c
            })
            .collect_vec();

        let mut in_order = FixedBitSet::with_capacity(schema.len());
        let mut order_keys = vec![];

        for field in &user_order_by.field_order {
            let idx = field.index;
            order_keys.push(field.clone());
            in_order.insert(idx);
        }

        for &idx in pk_indices {
            if in_order.contains(idx) {
                continue;
            }
            order_keys.push(FieldOrder {
                index: idx,
                direct: Direction::Asc,
            });
            in_order.insert(idx);
        }

        let ctx = input.ctx();
        let properties = ctx.inner().with_options.internal_table_subset();

        let table = TableCatalog {
            id: TableId::placeholder(),
            associated_source_id: None,
            name: mv_name,
            columns,
            order_key: order_keys,
            stream_key: pk_indices.clone(),
            is_index_on,
            distribution_key: base.dist.dist_column_indices().to_vec(),
            appendonly: input.append_only(),
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            properties,
            // TODO(zehua): replace it with FragmentId::placeholder()
            fragment_id: FragmentId::MAX - 1,
            vnode_col_idx: None,
        };

        Ok(Self { base, input, table })
    }

    /// Get a reference to the stream materialize's table.
    #[must_use]
    pub fn table(&self) -> &TableCatalog {
        &self.table
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }
}

impl fmt::Display for StreamMaterialize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = self.table();

        let column_names = table
            .columns()
            .iter()
            .map(|c| c.name_with_hidden())
            .join(", ");

        let pk_column_names = table
            .stream_key
            .iter()
            .map(|&pk| &table.columns[pk].column_desc.name)
            .join(", ");

        let order_descs = table
            .order_key
            .iter()
            .map(|order| table.columns()[order.index].column_desc.name.clone())
            .join(", ");

        let mut builder = f.debug_struct("StreamMaterialize");
        builder
            .field("columns", &format_args!("[{}]", column_names))
            .field("pk_columns", &format_args!("[{}]", pk_column_names));

        if pk_column_names != order_descs {
            builder.field("order_descs", &format_args!("[{}]", order_descs));
        }
        builder.finish()
    }
}

impl PlanTreeNodeUnary for StreamMaterialize {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let new = Self::new(input, self.table().clone());
        assert_eq!(new.plan_base().schema, self.plan_base().schema);
        assert_eq!(new.plan_base().logical_pk, self.plan_base().logical_pk);
        new
    }
}

impl_plan_tree_node_for_unary! { StreamMaterialize }

impl StreamNode for StreamMaterialize {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::Materialize(MaterializeNode {
            // We don't need table id for materialize node in frontend. The id will be generated on
            // meta catalog service.
            table_id: 0,
            column_orders: self
                .table()
                .order_key()
                .iter()
                .map(FieldOrder::to_protobuf)
                .collect(),
            // TODO(tesla): use id generated by state?
            table: Some(self.table().to_state_table_prost()),
        })
    }
}
