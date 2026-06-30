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

use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use risingwave_common::catalog::{ColumnCatalog, ColumnId};
use risingwave_common::constants::log_store::v2 as log_store_v2;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::{bail, dispatch_stream_node_body};
use risingwave_pb::catalog::{PbSink, PbTable};
use risingwave_pb::plan_common::PbField;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    self, PbStreamScanType, ProjectNode, SinkNode, StreamNode, StreamScanNode,
};

use crate::MetaResult;
use crate::controller::id::IdGeneratorManager;
use crate::model::{Fragment, FragmentId};
use crate::stream::stream_graph::fragment::{
    rewrite_project_node, rewrite_sink_node, rewrite_stream_scan_and_merge,
};
use crate::stream::stream_graph::id::GlobalFragmentIdGen;
use crate::stream::{SinkMetadataChange, TableMetadataUpdate};

pub fn check_sink_fragments_support_refresh_schema(
    fragments: &BTreeMap<FragmentId, Fragment>,
) -> MetaResult<()> {
    if fragments.len() != 1 {
        return Err(anyhow!(
            "sink with auto schema change should have only 1 fragment, but got {:?}",
            fragments.len()
        )
        .into());
    }
    let (_, fragment) = fragments.first_key_value().expect("non-empty");
    if !matches!(fragment.nodes.node_body.as_ref(), Some(NodeBody::Sink(_))) {
        return Err(anyhow!(
            "expect Sink node for auto schema change but got: {:?}",
            fragment.nodes.node_body
        )
        .into());
    }
    check_stream_node_schema_change_support(&fragment.nodes)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub enum TableSchemaChange {
    AddColumn { columns: Vec<ColumnCatalog> },
    DropColumn { column_ids: Vec<ColumnId> },
}

#[derive(Clone, Debug)]
pub(super) enum PropagatedSchemaChange {
    AddColumn(AddColumn),
    DropColumn(DropColumn),
}

impl PropagatedSchemaChange {
    pub(super) fn newly_added_fields(&self) -> &[PbField] {
        match self {
            Self::AddColumn(add_column) => &add_column.added_fields,
            Self::DropColumn(_) => &[],
        }
    }

    pub(super) fn is_drop_column(&self) -> bool {
        matches!(self, Self::DropColumn(_))
    }

    fn apply_fields(&self, fields: &[PbField]) -> MetaResult<Vec<PbField>> {
        match self {
            Self::AddColumn(add_column) => {
                let mut new_fields =
                    Vec::with_capacity(fields.len() + add_column.added_fields.len());
                new_fields.extend_from_slice(fields);
                new_fields.extend_from_slice(&add_column.added_fields);
                Ok(new_fields)
            }
            Self::DropColumn(drop_column) => {
                for &index in &drop_column.removed_indices {
                    if index >= fields.len() {
                        bail!(
                            "removed field index {} is out of range for fields {}",
                            index,
                            fields.len()
                        );
                    }
                }
                Ok(fields
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| !drop_column.removed_indices.contains(index))
                    .map(|(_, field)| field.clone())
                    .collect())
            }
        }
    }

    pub(super) fn remap_index(&self, old_index: usize, index_name: &str) -> MetaResult<usize> {
        match self {
            Self::AddColumn(_) => Ok(old_index),
            Self::DropColumn(drop_column) => {
                remap_index_after_drop(old_index, &drop_column.removed_indices).ok_or_else(|| {
                    anyhow!("cannot drop {} column at index {}", index_name, old_index).into()
                })
            }
        }
    }
}

pub(super) fn remap_index_after_drop(old_index: usize, removed_indices: &[usize]) -> Option<usize> {
    if removed_indices.contains(&old_index) {
        return None;
    }
    let removed_before = removed_indices
        .iter()
        .filter(|&&removed_index| removed_index < old_index)
        .count();
    Some(old_index - removed_before)
}

#[derive(Clone, Debug)]
pub(super) struct AddColumn {
    /// Logical fields appended by this change. `StreamScan` reads upstream
    /// table-specific catalog metadata, such as column ids, from the collector's
    /// new upstream table.
    pub added_fields: Vec<PbField>,
}

#[derive(Clone, Debug)]
pub(super) struct DropColumn {
    /// Old output indices removed by this change.
    pub removed_indices: Vec<usize>,
}

struct SinkSchemaChangeCollector<'a> {
    new_upstream_table: &'a PbTable,
    upstream_table_change: &'a TableSchemaChange,
    upstream_table_fragment_id: FragmentId,
    original_sink: &'a PbSink,
    sink_metadata_change: &'a mut Option<SinkMetadataChange>,
    updated_tables: &'a mut Vec<TableMetadataUpdate>,
}

impl<'a> SinkSchemaChangeCollector<'a> {
    fn reborrow<'b>(&'b mut self) -> SinkSchemaChangeCollector<'b> {
        SinkSchemaChangeCollector {
            new_upstream_table: self.new_upstream_table,
            upstream_table_change: self.upstream_table_change,
            upstream_table_fragment_id: self.upstream_table_fragment_id,
            original_sink: self.original_sink,
            sink_metadata_change: &mut *self.sink_metadata_change,
            updated_tables: &mut *self.updated_tables,
        }
    }
}

pub(super) fn validate_log_store_schema_prefix(log_store_table: &PbTable) -> MetaResult<usize> {
    let predefined_len = log_store_v2::KV_LOG_STORE_PREDEFINED_COLUMNS.len();
    if log_store_table.columns.len() < predefined_len {
        bail!(
            "invalid log store table column count {} for sink schema change",
            log_store_table.columns.len()
        );
    }
    for (column, (expected_name, expected_type)) in log_store_table.columns[..predefined_len]
        .iter()
        .zip_eq_fast(log_store_v2::KV_LOG_STORE_PREDEFINED_COLUMNS.iter())
    {
        let column_desc = column.column_desc.as_ref().unwrap();
        if column_desc.name != *expected_name {
            bail!(
                "log store predefined column name {} does not match expected {}",
                column_desc.name,
                expected_name
            );
        }
        let column_type = DataType::from(column_desc.column_type.as_ref().unwrap());
        if !column_type.equals_datatype(expected_type) {
            bail!(
                "log store predefined column type mismatch for column {}",
                column_desc.name
            );
        }
    }
    Ok(predefined_len)
}

pub(super) fn sink_node_supports_log_store_schema_change(sink_node: &SinkNode) -> bool {
    sink_node
        .table
        .as_ref()
        .is_none_or(|log_store_table| validate_log_store_schema_prefix(log_store_table).is_ok())
}

pub(super) struct NodeSchemaRewrite {
    pub body: NodeBody,
    pub identity: Option<String>,
    pub change: Option<PropagatedSchemaChange>,
}

trait SinkSchemaChangeSupport {
    fn supports_schema_change(&self) -> bool {
        false
    }

    fn apply_schema_change(
        self,
        _input_changes: Vec<(Option<PropagatedSchemaChange>, Vec<PbField>)>,
        _collector: SinkSchemaChangeCollector<'_>,
    ) -> MetaResult<NodeSchemaRewrite>
    where
        Self: Sized,
    {
        Err(anyhow!(
            "{} node does not support sink auto schema change",
            std::any::type_name::<Self>()
        )
        .into())
    }
}

impl SinkSchemaChangeSupport for stream_plan::MergeNode {
    fn supports_schema_change(&self) -> bool {
        true
    }

    fn apply_schema_change(
        self,
        _input_changes: Vec<(Option<PropagatedSchemaChange>, Vec<PbField>)>,
        _collector: SinkSchemaChangeCollector<'_>,
    ) -> MetaResult<NodeSchemaRewrite> {
        Err(anyhow!("Merge node should be rewritten together with StreamScan node").into())
    }
}

impl SinkSchemaChangeSupport for StreamScanNode {
    fn supports_schema_change(&self) -> bool {
        self.stream_scan_type == PbStreamScanType::ArrangementBackfill as i32
    }

    fn apply_schema_change(
        self,
        _input_changes: Vec<(Option<PropagatedSchemaChange>, Vec<PbField>)>,
        _collector: SinkSchemaChangeCollector<'_>,
    ) -> MetaResult<NodeSchemaRewrite> {
        Err(anyhow!("StreamScan node should be rewritten together with Merge node").into())
    }
}

impl SinkSchemaChangeSupport for ProjectNode {
    fn supports_schema_change(&self) -> bool {
        true
    }

    fn apply_schema_change(
        self,
        input_changes: Vec<(Option<PropagatedSchemaChange>, Vec<PbField>)>,
        _collector: SinkSchemaChangeCollector<'_>,
    ) -> MetaResult<NodeSchemaRewrite> {
        if input_changes.len() != 1 {
            return Err(anyhow!(
                "Project node should have exactly one input for sink schema change, but got {}",
                input_changes.len()
            )
            .into());
        }
        let [(input_change, input_fields)] = input_changes.as_slice() else {
            unreachable!("Project input length is checked above")
        };
        let Some(input_change) = input_change else {
            return Ok(NodeSchemaRewrite {
                body: NodeBody::Project(Box::new(self)),
                identity: None,
                change: None,
            });
        };
        rewrite_project_node(self, input_change.clone(), input_fields)
    }
}

impl SinkSchemaChangeSupport for SinkNode {
    fn supports_schema_change(&self) -> bool {
        sink_node_supports_log_store_schema_change(self)
    }

    fn apply_schema_change(
        self,
        input_changes: Vec<(Option<PropagatedSchemaChange>, Vec<PbField>)>,
        collector: SinkSchemaChangeCollector<'_>,
    ) -> MetaResult<NodeSchemaRewrite> {
        if input_changes.len() != 1 {
            return Err(anyhow!(
                "Sink node should have exactly one input for sink schema change, but got {}",
                input_changes.len()
            )
            .into());
        }
        let [(input_change, input_fields)] = input_changes.as_slice() else {
            unreachable!("Sink input length is checked above")
        };
        let Some(input_change) = input_change else {
            return Ok(NodeSchemaRewrite {
                body: NodeBody::Sink(Box::new(self)),
                identity: None,
                change: None,
            });
        };
        let (rewrite, sink_metadata_change, updated_table) = rewrite_sink_node(
            self,
            input_change.clone(),
            input_fields,
            collector.original_sink,
        )?;
        if let Some(updated_table) = updated_table {
            collector.updated_tables.push(updated_table);
        }
        *collector.sink_metadata_change = Some(sink_metadata_change);
        Ok(rewrite)
    }
}

impl SinkSchemaChangeSupport for stream_plan::BatchPlanNode {
    fn supports_schema_change(&self) -> bool {
        true
    }

    fn apply_schema_change(
        self,
        _input_changes: Vec<(Option<PropagatedSchemaChange>, Vec<PbField>)>,
        _collector: SinkSchemaChangeCollector<'_>,
    ) -> MetaResult<NodeSchemaRewrite> {
        Err(anyhow!("BatchPlan node should be rewritten together with StreamScan node").into())
    }
}

macro_rules! impl_unsupported_schema_change {
    ($($ty:ty),* $(,)?) => {
        $(
            impl SinkSchemaChangeSupport for $ty {}
        )*
    };
}

impl_unsupported_schema_change!(
    stream_plan::SourceNode,
    stream_plan::FilterNode,
    stream_plan::MaterializeNode,
    stream_plan::SimpleAggNode,
    stream_plan::HashAggNode,
    stream_plan::TopNNode,
    stream_plan::HashJoinNode,
    stream_plan::HopWindowNode,
    stream_plan::ExchangeNode,
    stream_plan::LookupNode,
    stream_plan::ArrangeNode,
    stream_plan::LookupUnionNode,
    stream_plan::UnionNode,
    stream_plan::DeltaIndexJoinNode,
    stream_plan::ExpandNode,
    stream_plan::DynamicFilterNode,
    stream_plan::ProjectSetNode,
    stream_plan::GroupTopNNode,
    stream_plan::SortNode,
    stream_plan::WatermarkFilterNode,
    stream_plan::DmlNode,
    stream_plan::RowIdGenNode,
    stream_plan::NowNode,
    stream_plan::TemporalJoinNode,
    stream_plan::BarrierRecvNode,
    stream_plan::ValuesNode,
    stream_plan::DedupNode,
    stream_plan::NoOpNode,
    stream_plan::EowcOverWindowNode,
    stream_plan::OverWindowNode,
    stream_plan::StreamFsFetchNode,
    stream_plan::StreamCdcScanNode,
    stream_plan::CdcFilterNode,
    stream_plan::SourceBackfillNode,
    stream_plan::ChangeLogNode,
    stream_plan::LocalApproxPercentileNode,
    stream_plan::GlobalApproxPercentileNode,
    stream_plan::RowMergeNode,
    stream_plan::AsOfJoinNode,
    stream_plan::SyncLogStoreNode,
    stream_plan::MaterializedExprsNode,
    stream_plan::VectorIndexWriteNode,
    stream_plan::UpstreamSinkUnionNode,
    stream_plan::LocalityProviderNode,
    stream_plan::EowcGapFillNode,
    stream_plan::GapFillNode,
    stream_plan::VectorIndexLookupJoinNode,
    stream_plan::IcebergWithPkIndexWriterNode,
    stream_plan::IcebergWithPkIndexDvMergerNode,
);

fn node_body_name(body: &NodeBody) -> &'static str {
    dispatch_stream_node_body!(body, _NodeVariant, node => std::any::type_name_of_val(node))
}

fn node_supports_schema_change(body: &NodeBody) -> bool {
    dispatch_stream_node_body!(body, _NodeVariant, node => {
        node.supports_schema_change()
    })
}

fn check_stream_node_schema_change_support(node: &StreamNode) -> MetaResult<()> {
    let body = node
        .node_body
        .as_ref()
        .ok_or_else(|| anyhow!("stream node body is missing"))?;

    for input_node in &node.input {
        check_stream_node_schema_change_support(input_node)?;
    }
    if !node_supports_schema_change(body) {
        return Err(anyhow!(
            "{} node {} does not support sink schema change",
            node_body_name(body),
            node.operator_id
        )
        .into());
    }
    Ok(())
}

fn rewrite_stream_node_for_sink_schema_change(
    node: StreamNode,
    mut collector: SinkSchemaChangeCollector<'_>,
) -> MetaResult<(StreamNode, Option<PropagatedSchemaChange>)> {
    if matches!(node.node_body.as_ref(), Some(NodeBody::StreamScan(_))) {
        let mut stream_scan_node = node;
        let old_stream_key = stream_scan_node.stream_key.clone();
        let change = rewrite_stream_scan_and_merge(
            &mut stream_scan_node,
            collector.upstream_table_change,
            collector.new_upstream_table,
            collector.upstream_table_fragment_id,
        )?;
        if let Some(change) = &change {
            stream_scan_node.stream_key = old_stream_key
                .into_iter()
                .map(|index| {
                    change
                        .remap_index(index as usize, "stream key")
                        .map(|index| index as u32)
                })
                .collect::<MetaResult<_>>()?;
        }
        return Ok((stream_scan_node, change));
    }

    let StreamNode {
        operator_id,
        input,
        stream_key,
        stream_kind,
        identity,
        fields,
        node_body,
    } = node;
    let body = node_body.ok_or_else(|| anyhow!("stream node body is missing"))?;

    let mut new_inputs = Vec::with_capacity(input.len());
    let mut input_changes = vec![];
    for input_node in input {
        let (new_input, input_change) =
            rewrite_stream_node_for_sink_schema_change(input_node, collector.reborrow())?;
        let input_fields = new_input.fields.clone();
        new_inputs.push(new_input);
        input_changes.push((input_change, input_fields));
    }
    let node_name = node_body_name(&body);
    let rewrite = dispatch_stream_node_body!(body, _NodeVariant, node => {
        node.apply_schema_change(input_changes, collector.reborrow())
            .with_context(|| {
                format!(
                    "failed to rewrite {} stream node {}",
                    node_name, operator_id
                )
            })
    })?;
    let (fields, stream_key) = if let Some(change) = &rewrite.change {
        (
            change.apply_fields(&fields)?,
            stream_key
                .into_iter()
                .map(|index| {
                    change
                        .remap_index(index as usize, "stream key")
                        .map(|index| index as u32)
                })
                .collect::<MetaResult<_>>()?,
        )
    } else {
        (fields, stream_key)
    };

    Ok((
        StreamNode {
            operator_id,
            input: new_inputs,
            stream_key,
            stream_kind,
            identity: rewrite.identity.unwrap_or(identity),
            fields,
            node_body: Some(rewrite.body),
        },
        rewrite.change,
    ))
}

pub fn rewrite_refresh_schema_sink_fragment(
    original_sink_fragment: &Fragment,
    sink: &PbSink,
    schema_change: TableSchemaChange,
    upstream_table: &PbTable,
    upstream_table_fragment_id: FragmentId,
    id_generator_manager: &IdGeneratorManager,
) -> MetaResult<(Fragment, SinkMetadataChange, Vec<TableMetadataUpdate>)> {
    let root_body = original_sink_fragment
        .nodes
        .node_body
        .as_ref()
        .ok_or_else(|| anyhow!("stream node body is missing"))?;
    if !matches!(root_body, NodeBody::Sink(_)) {
        return Err(anyhow!(
            "expect Sink node for auto schema change but got {}",
            node_body_name(root_body)
        )
        .into());
    }
    let fragment_id = GlobalFragmentIdGen::new(id_generator_manager, 1)
        .to_global_id(0)
        .as_global_id();
    let mut sink_metadata_change = None;
    let mut updated_tables = vec![];

    let collector = SinkSchemaChangeCollector {
        new_upstream_table: upstream_table,
        upstream_table_change: &schema_change,
        upstream_table_fragment_id,
        original_sink: sink,
        sink_metadata_change: &mut sink_metadata_change,
        updated_tables: &mut updated_tables,
    };
    let (new_nodes, output_change) = rewrite_stream_node_for_sink_schema_change(
        original_sink_fragment.nodes.clone(),
        collector,
    )?;
    if output_change.is_none() {
        tracing::warn!("sink schema change did not affect the Sink node while rewriting fragment");
    }

    let sink_metadata_change = sink_metadata_change.unwrap_or_else(|| SinkMetadataChange {
        columns: sink.columns.clone(),
        plan_pk: sink.plan_pk.clone(),
        distribution_key: sink.distribution_key.clone(),
        downstream_pk: sink.downstream_pk.clone(),
    });
    let new_sink_fragment = Fragment {
        fragment_id,
        fragment_type_mask: original_sink_fragment.fragment_type_mask,
        distribution_type: original_sink_fragment.distribution_type,
        state_table_ids: original_sink_fragment.state_table_ids.clone(),
        maybe_vnode_count: original_sink_fragment.maybe_vnode_count,
        nodes: new_nodes,
    };
    Ok((new_sink_fragment, sink_metadata_change, updated_tables))
}

#[cfg(test)]
mod tests {
    use super::remap_index_after_drop;

    #[test]
    fn test_remap_index_after_drop() {
        assert_eq!(remap_index_after_drop(0, &[1, 3]), Some(0));
        assert_eq!(remap_index_after_drop(1, &[1, 3]), None);
        assert_eq!(remap_index_after_drop(2, &[1, 3]), Some(1));
        assert_eq!(remap_index_after_drop(3, &[1, 3]), None);
        assert_eq!(remap_index_after_drop(4, &[1, 3]), Some(2));

        assert_eq!(remap_index_after_drop(0, &[3, 1]), Some(0));
        assert_eq!(remap_index_after_drop(1, &[3, 1]), None);
        assert_eq!(remap_index_after_drop(2, &[3, 1]), Some(1));
        assert_eq!(remap_index_after_drop(3, &[3, 1]), None);
        assert_eq!(remap_index_after_drop(4, &[3, 1]), Some(2));
    }
}
