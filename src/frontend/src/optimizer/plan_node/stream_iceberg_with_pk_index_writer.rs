// Copyright 2026 RisingWave Labs
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

use pretty_xmlish::XmlNode;
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_pb::stream_plan::IcebergWithPkIndexWriterNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::stream::prelude::*;
use crate::TableCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{Distill, TableCatalogBuilder, childless_record};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, StreamPlanRef as PlanRef,
};
use crate::optimizer::property::{
    Distribution, FunctionalDependencySet, MonotonicityMap, WatermarkColumns,
};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamIcebergWithPkIndexWriter` is the stateful writer executor for the Iceberg
/// with pk index sink. It maintains a PK index and writes data files to Iceberg.
///
/// Output schema: `[file_path: Varchar, position: Int64]` — delete-position info for
/// the downstream `DvMerger`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamIcebergWithPkIndexWriter {
    pub base: PlanBase<Stream>,
    pub input: PlanRef,
    pub sink_desc: SinkDesc,
    pub pk_index_table: TableCatalog,
}

impl StreamIcebergWithPkIndexWriter {
    pub fn new(input: PlanRef, sink_desc: SinkDesc) -> Self {
        let pk_index_table = build_iceberg_pk_state_table(&sink_desc);
        Self::new_with_pk_index_table(input, sink_desc, pk_index_table)
    }

    fn new_with_pk_index_table(
        input: PlanRef,
        sink_desc: SinkDesc,
        pk_index_table: TableCatalog,
    ) -> Self {
        let output_schema = output_schema();
        let fd_set = FunctionalDependencySet::new(output_schema.len());
        let distribution = match input.distribution() {
            Distribution::Single => Distribution::Single,
            _ => Distribution::SomeShard,
        };
        let base = PlanBase::new_stream(
            input.ctx(),
            output_schema,
            Some(vec![]), // no stream key
            fd_set,
            distribution,
            StreamKind::AppendOnly,
            input.emit_on_window_close(),
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );
        Self {
            base,
            input,
            sink_desc,
            pk_index_table,
        }
    }
}

fn output_schema() -> risingwave_common::catalog::Schema {
    risingwave_common::catalog::Schema::new(vec![
        Field::with_name(DataType::Varchar, "file_path"),
        Field::with_name(DataType::Int64, "position"),
    ])
}

fn build_iceberg_pk_state_table(sink_desc: &SinkDesc) -> TableCatalog {
    let mut builder = TableCatalogBuilder::default();

    for order in &sink_desc.plan_pk {
        builder.add_column(&Field::from(
            &sink_desc.columns[order.column_index].column_desc,
        ));
    }
    builder.add_column(&Field::with_name(DataType::Varchar, "file_path"));
    builder.add_column(&Field::with_name(DataType::Int64, "position"));

    for (idx, order) in sink_desc.plan_pk.iter().enumerate() {
        builder.add_order_column(idx, order.order_type);
    }

    builder.build(
        (0..sink_desc.plan_pk.len()).collect(),
        sink_desc.plan_pk.len(),
    )
}

impl Distill for StreamIcebergWithPkIndexWriter {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamIcebergWithPkIndexWriter", vec![])
    }
}

impl PlanTreeNodeUnary<Stream> for StreamIcebergWithPkIndexWriter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new_with_pk_index_table(input, self.sink_desc.clone(), self.pk_index_table.clone())
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamIcebergWithPkIndexWriter }

impl StreamNode for StreamIcebergWithPkIndexWriter {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let pk_index_table = self
            .pk_index_table
            .clone()
            .with_id(state.gen_table_id_wrapped());

        NodeBody::IcebergWithPkIndexWriter(Box::new(IcebergWithPkIndexWriterNode {
            sink_desc: Some(self.sink_desc.to_proto()),
            pk_index_table: Some(pk_index_table.to_internal_table_prost()),
        }))
    }
}

impl ExprRewritable<Stream> for StreamIcebergWithPkIndexWriter {}

impl ExprVisitable for StreamIcebergWithPkIndexWriter {}
