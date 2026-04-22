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
use risingwave_common::util::sort_util::OrderType;
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
/// Output schema: `[file_path: Varchar, payload: Bytea]`, where `payload` is encoded as `Payload` struct, which can be either:
/// 1. Position information for delete operations, or
/// 2. Partition metadata for the written data file (if partitioned).
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
        Field::with_name(DataType::Bytea, "payload"),
    ])
}

fn build_iceberg_pk_state_table(sink_desc: &SinkDesc) -> Result<TableCatalog> {
    let mut builder = TableCatalogBuilder::default();

    let downstream_pk = sink_desc
        .downstream_pk
        .as_ref()
        .context("Missing downstream PK in Iceberg sink desc")?;
    for &idx in downstream_pk {
        builder.add_column(&Field::from(&sink_desc.columns[idx].column_desc));
    }
    builder.add_column(&Field::with_name(DataType::Varchar, "file_path"));
    builder.add_column(&Field::with_name(DataType::Int64, "position"));

    for idx in 0..downstream_pk.len() {
        builder.add_order_column(idx, OrderType::ascending());
    }

    let res = builder.build((0..downstream_pk.len()).collect(), downstream_pk.len());
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{
        ColumnCatalog, ColumnDesc, ColumnId, CreateType, DEFAULT_SUPER_USER_ID, StreamJobStatus,
    };
    use risingwave_common::util::sort_util::ColumnOrder;
    use risingwave_connector::sink::catalog::{SinkId, SinkType};

    use super::*;

    fn test_sink_desc() -> SinkDesc {
        SinkDesc {
            id: SinkId::placeholder(),
            name: "s".to_owned(),
            definition: "".to_owned(),
            columns: vec![
                ColumnCatalog::visible(ColumnDesc::named("id", ColumnId::new(0), DataType::Int32)),
                ColumnCatalog::visible(ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32)),
                ColumnCatalog::hidden(ColumnDesc::named(
                    "_row_id",
                    ColumnId::new(2),
                    DataType::Serial,
                )),
            ],
            plan_pk: vec![ColumnOrder::new(2, OrderType::ascending())],
            downstream_pk: Some(vec![1]),
            distribution_key: vec![1],
            properties: BTreeMap::new(),
            secret_refs: BTreeMap::new(),
            sink_type: SinkType::Upsert,
            ignore_delete: false,
            format_desc: None,
            db_name: "dev".to_owned(),
            sink_from_name: "t".to_owned(),
            target_table: None,
            extra_partition_col_idx: None,
            create_type: CreateType::Foreground,
            is_exactly_once: None,
            auto_refresh_schema_from_table: None,
        }
    }

    #[test]
    fn test_build_iceberg_pk_state_table_uses_downstream_pk_columns() {
        let table = build_iceberg_pk_state_table(&test_sink_desc()).unwrap();

        assert_eq!(table.columns()[0].name(), "v1");
        assert_eq!(table.columns()[1].name(), "file_path");
        assert_eq!(table.columns()[2].name(), "position");
        assert_eq!(table.pk().len(), 1);
        assert_eq!(table.pk()[0].column_index, 0);
        assert_eq!(table.distribution_key(), &[0]);
        assert_eq!(table.read_prefix_len_hint, 1);
        assert_eq!(table.owner, DEFAULT_SUPER_USER_ID);
        assert_eq!(table.stream_job_status, StreamJobStatus::Creating);
    }
}
