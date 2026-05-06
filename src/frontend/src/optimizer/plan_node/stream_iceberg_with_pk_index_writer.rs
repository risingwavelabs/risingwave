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

use anyhow::Context;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_pb::stream_plan::IcebergWithPkIndexWriterNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::stream::prelude::*;
use crate::TableCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{
    Distill, IndicesDisplay, TableCatalogBuilder, childless_record,
};
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
    pub fn from_stream_sink(sink: &super::StreamSink) -> Result<Self> {
        let output_schema = output_schema();
        let fd_set = FunctionalDependencySet::new(output_schema.len());
        let dist = match sink.distribution() {
            Distribution::Single => Distribution::Single,
            _ => Distribution::SomeShard,
        };
        let base = PlanBase::new_stream(
            sink.ctx(),
            output_schema,
            Some(vec![]),
            fd_set,
            dist,
            StreamKind::AppendOnly,
            sink.emit_on_window_close(),
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );
        let pk_index_table = build_iceberg_pk_state_table(sink.sink_desc())?;
        Ok(Self {
            base,
            input: sink.input(),
            sink_desc: sink.sink_desc().clone(),
            pk_index_table,
        })
    }
}

fn output_schema() -> risingwave_common::catalog::Schema {
    risingwave_common::catalog::Schema::new(vec![
        Field::with_name(DataType::Varchar, "file_path"),
        Field::with_name(DataType::Int64, "position"),
    ])
}

fn build_iceberg_pk_state_table(sink_desc: &SinkDesc) -> Result<TableCatalog> {
    let mut builder = TableCatalogBuilder::default();

    let downstream_pk = sink_desc
        .downstream_pk
        .as_ref()
        .context("Missing downstream PK in Iceberg sink desc")?;
    for &idx in downstream_pk {
        let order = &sink_desc.plan_pk[idx];
        builder.add_column(&Field::from(
            &sink_desc.columns[order.column_index].column_desc,
        ));
    }
    builder.add_column(&Field::with_name(DataType::Varchar, "file_path"));
    builder.add_column(&Field::with_name(DataType::Int64, "position"));

    for (idx, order) in sink_desc.plan_pk.iter().enumerate() {
        builder.add_order_column(idx, order.order_type);
    }

    let res = builder.build(
        (0..sink_desc.plan_pk.len()).collect(),
        sink_desc.plan_pk.len(),
    );
    Ok(res)
}

impl Distill for StreamIcebergWithPkIndexWriter {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let column_names = self
            .sink_desc
            .columns
            .iter()
            .map(|col| col.name_with_hidden().to_string())
            .map(Pretty::from)
            .collect();
        let column_names = Pretty::Array(column_names);
        let mut vec = Vec::with_capacity(2);
        vec.push(("columns", column_names));
        if let Some(pk) = &self.sink_desc.downstream_pk {
            let sink_pk = IndicesDisplay {
                indices: pk,
                schema: self.input.schema(),
            };
            vec.push(("downstream_pk", sink_pk.distill()));
        }

        childless_record("StreamIcebergWithPkIndexWriter", vec)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamIcebergWithPkIndexWriter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            base: self.base.clone(),
            input,
            sink_desc: self.sink_desc.clone(),
            pk_index_table: self.pk_index_table.clone(),
        }
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
