// Copyright 2023 Singularity Data
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

use std::assert_matches::assert_matches;
use std::collections::HashSet;
use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{PlanRef, PlanTreeNodeUnary, StreamNode, StreamSink};
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::table_catalog::{TableCatalog, TableType, TableVersion};
use crate::catalog::{FragmentId, USER_COLUMN_ID_OFFSET};
use crate::optimizer::plan_node::{PlanBase, PlanNode};
use crate::optimizer::property::{Direction, Distribution, FieldOrder, Order, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::WithOptions;

/// Materializes a stream.
#[derive(Debug, Clone)]
pub struct StreamMaterialize {
    pub base: PlanBase,
    /// Child of Materialize plan
    input: PlanRef,
    table: TableCatalog,
}

impl StreamMaterialize {
    #[must_use]
    pub fn new(input: PlanRef, table: TableCatalog) -> Self {
        let base = PlanBase::derive_stream_plan_base(&input);
        Self { base, input, table }
    }

    /// Create a materialize node, for `MATERIALIZED VIEW`, `INDEX`, and `SINK`.
    ///
    /// When creating index, `TableType` should be `Index`. Then, materialize will distribute keys
    /// using `user_distributed_by`.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input: PlanRef,
        name: String,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        user_cols: FixedBitSet,
        out_names: Vec<String>,
        definition: String,
        table_type: TableType,
    ) -> Result<Self> {
        let input = Self::rewrite_input(input, user_distributed_by, table_type)?;
        let schema = input.schema();

        // Used to validate and deduplicate column names.
        let mut col_names = HashSet::new();
        for name in &out_names {
            if !col_names.insert(name.clone()) {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{}\" specified more than once",
                    name
                )))?;
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
                        i as i32 + USER_COLUMN_ID_OFFSET,
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
                        name = format!("{}#{}", field.name, count);
                    }

                    name
                };
                c
            })
            .collect_vec();

        let table = Self::derive_table_catalog(
            input.clone(),
            name,
            user_order_by,
            columns,
            definition,
            false,
            None,
            table_type,
        )?;

        Ok(Self::new(input, table))
    }

    /// Create a materialize node, for `TABLE`.
    ///
    /// Different from `create`, the `columns` are passed in directly, instead of being derived from
    /// the input. So the column IDs are preserved from the SQL columns binding step and will be
    /// consistent with the source node and DML node.
    #[allow(clippy::too_many_arguments)]
    pub fn create_for_table(
        input: PlanRef,
        name: String,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        handle_pk_conflict: bool,
        row_id_index: Option<usize>,
    ) -> Result<Self> {
        let input = Self::rewrite_input(input, user_distributed_by, TableType::Table)?;

        let table = Self::derive_table_catalog(
            input.clone(),
            name,
            user_order_by,
            columns,
            definition,
            handle_pk_conflict,
            row_id_index,
            TableType::Table,
        )?;

        Ok(Self::new(input, table))
    }

    /// Rewrite the input to satisfy the required distribution if necessary, according to the type.
    fn rewrite_input(
        input: PlanRef,
        user_distributed_by: RequiredDist,
        table_type: TableType,
    ) -> Result<PlanRef> {
        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => match table_type {
                TableType::Table | TableType::MaterializedView => {
                    assert_matches!(user_distributed_by, RequiredDist::Any);
                    // ensure the same pk will not shuffle to different node
                    RequiredDist::shard_by_key(input.schema().len(), input.logical_pk())
                }
                TableType::Index => {
                    assert_matches!(
                        user_distributed_by,
                        RequiredDist::PhysicalDist(Distribution::HashShard(_))
                    );
                    user_distributed_by
                }
                TableType::Internal => unreachable!(),
            },
        };

        required_dist.enforce_if_not_satisfies(input, &Order::any())
    }

    /// Derive the table catalog with the given arguments.
    ///
    /// - The caller must ensure the validity of the given `columns`.
    /// - The `rewritten_input` should be generated by `rewrite_input`.
    #[allow(clippy::too_many_arguments)]
    fn derive_table_catalog(
        rewritten_input: PlanRef,
        name: String,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        handle_pk_conflict: bool,
        row_id_index: Option<usize>,
        table_type: TableType,
    ) -> Result<TableCatalog> {
        let input = rewritten_input;

        // Note(congyi): avoid pk duplication
        let pk_indices = input.logical_pk().iter().copied().unique().collect_vec();
        let schema = input.schema();
        let distribution = input.distribution();

        // Assert the uniqueness of column names and IDs, including hidden columns.
        if let Some(name) = columns.iter().map(|c| c.name()).duplicates().next() {
            panic!("duplicated column name \"{name}\"");
        }
        if let Some(id) = columns.iter().map(|c| c.column_id()).duplicates().next() {
            panic!("duplicated column ID {id}");
        }
        // Assert that the schema of given `columns` is correct.
        assert_eq!(
            columns.iter().map(|c| c.data_type().clone()).collect_vec(),
            input.schema().data_types()
        );

        let value_indices = (0..columns.len()).collect_vec();
        let mut in_order = FixedBitSet::with_capacity(schema.len());
        let mut pk_list = vec![];

        for field in &user_order_by.field_order {
            let idx = field.index;
            pk_list.push(field.clone());
            in_order.insert(idx);
        }

        for &idx in &pk_indices {
            if in_order.contains(idx) {
                continue;
            }
            pk_list.push(FieldOrder {
                index: idx,
                direct: Direction::Asc,
            });
            in_order.insert(idx);
        }

        // Always use the initial version for now.
        let max_column_id = columns.iter().map(|c| c.column_id()).max().unwrap();
        let initial_version = TableVersion::initial(max_column_id);

        let distribution_key = distribution.dist_column_indices().to_vec();
        let properties = input.ctx().with_options().internal_table_subset(); // TODO: remove this
        let read_prefix_len_hint = pk_indices.len();

        Ok(TableCatalog {
            id: TableId::placeholder(),
            associated_source_id: None,
            name,
            columns,
            pk: pk_list,
            stream_key: pk_indices,
            distribution_key,
            table_type,
            append_only: input.append_only(),
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            properties,
            // TODO(zehua): replace it with FragmentId::placeholder()
            fragment_id: FragmentId::MAX - 1,
            vnode_col_index: None,
            row_id_index,
            value_indices,
            definition,
            handle_pk_conflict,
            read_prefix_len_hint,
            version: Some(initial_version),
        })
    }

    /// Get a reference to the stream materialize's table.
    #[must_use]
    pub fn table(&self) -> &TableCatalog {
        &self.table
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Rewrite this plan node into [`StreamSink`] with the given `properties`.
    pub fn rewrite_into_sink(self, properties: WithOptions) -> StreamSink {
        let Self {
            base,
            input,
            mut table,
        } = self;
        table.properties = properties;
        StreamSink::with_base(input, table, base)
    }
}

impl fmt::Display for StreamMaterialize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            .pk
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
                .pk()
                .iter()
                .map(FieldOrder::to_protobuf)
                .collect(),
            table: Some(self.table().to_internal_table_prost()),
            handle_pk_conflict: self.table.handle_pk_conflict(),
        })
    }
}
