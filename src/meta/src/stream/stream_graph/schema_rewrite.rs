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

use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_connector::sink::catalog::SinkType;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::expr::PbExprNode;
use risingwave_pb::expr::expr_node::PbRexNode;
use risingwave_pb::plan_common::{PbColumnCatalog, PbField};
use risingwave_pb::stream_plan::stream_node::{NodeBody as PbNodeBody, NodeBodyDiscriminants};
use risingwave_pb::stream_plan::{PbStreamScanType, StreamNode};
use strum::IntoDiscriminant;

use crate::MetaResult;
use crate::model::FragmentId;

pub enum SinkSchemaChangeSet {
    AddColumns(Vec<ColumnCatalog>),
    #[allow(dead_code)]
    DropColumns(Vec<String>),
    // Placeholder for future extensibility.
    // AlterColumnTypes,
}

impl SinkSchemaChangeSet {
    pub fn for_add_columns(newly_added_columns: &[ColumnCatalog]) -> Self {
        Self::AddColumns(newly_added_columns.to_vec())
    }

    #[allow(dead_code)]
    pub fn for_drop_columns(dropped_column_names: &[String]) -> Self {
        Self::DropColumns(dropped_column_names.to_vec())
    }
}

pub struct RewriteContext<'a> {
    pub sink: &'a risingwave_pb::catalog::PbSink,
    pub upstream_table: &'a PbTable,
    pub upstream_table_fragment_id: FragmentId,
    pub fragment_id_mapping: &'a HashMap<FragmentId, FragmentId>,
}

pub trait OperatorSchemaChangeRule: Send + Sync {
    /// Validates whether this operator node is supported by the current rewrite framework.
    fn validate(&self, _node: &StreamNode) -> MetaResult<()> {
        Ok(())
    }

    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()>;
}

pub struct SchemaChangePlanRewriter {
    rules: HashMap<NodeBodyDiscriminants, Box<dyn OperatorSchemaChangeRule>>,
}

impl SchemaChangePlanRewriter {
    pub fn rewrite_root(
        &self,
        root: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        self.rewrite_node(root, change_set, ctx)
    }

    pub fn validate_root(&self, root: &StreamNode) -> MetaResult<()> {
        self.validate_node(root)
    }

    fn rewrite_node(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        for child in &mut node.input {
            self.rewrite_node(child, change_set, ctx)?;
        }

        let kind = node
            .node_body
            .as_ref()
            .expect("stream node should have a node_body")
            .discriminant();
        let Some(rule) = self.rules.get(&kind) else {
            return Err(
                anyhow!("unsupported operator for sink auto schema change: kind={kind}").into(),
            );
        };
        rule.rewrite(node, change_set, ctx)
    }

    fn validate_node(&self, node: &StreamNode) -> MetaResult<()> {
        for child in &node.input {
            self.validate_node(child)?;
        }

        let kind = node
            .node_body
            .as_ref()
            .expect("stream node should have a node_body")
            .discriminant();
        let Some(rule) = self.rules.get(&kind) else {
            return Err(
                anyhow!("unsupported operator for sink auto schema change: kind={kind}").into(),
            );
        };
        rule.validate(node)
    }
}

/// Default rule set for sink auto schema change rewriting.
pub static DEFAULT_SINK_SCHEMA_REWRITER: LazyLock<SchemaChangePlanRewriter> = LazyLock::new(|| {
    let entries: [(NodeBodyDiscriminants, Box<dyn OperatorSchemaChangeRule>); 5] = [
        (NodeBodyDiscriminants::Sink, Box::new(SinkRule)),
        (NodeBodyDiscriminants::Project, Box::new(ProjectRule)),
        (NodeBodyDiscriminants::StreamScan, Box::new(StreamScanRule)),
        (NodeBodyDiscriminants::Merge, Box::new(MergeRule)),
        (NodeBodyDiscriminants::BatchPlan, Box::new(BatchPlanRule)),
    ];
    let rules = HashMap::from(entries);

    SchemaChangePlanRewriter { rules }
});

fn extend_pb_fields(
    fields: &mut Vec<PbField>,
    new_columns: &[ColumnCatalog],
    table_name: &str,
) -> MetaResult<()> {
    fields.extend(new_columns.iter().map(|col| {
        Field::new(
            format!("{}.{}", table_name, col.column_desc.name),
            col.data_type().clone(),
        )
        .to_prost()
    }));

    Ok(())
}

pub struct SinkRule;

impl OperatorSchemaChangeRule for SinkRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        fn extend_sink_columns(
            sink_columns: &mut Vec<PbColumnCatalog>,
            new_columns: &[ColumnCatalog],
            get_column_name: impl Fn(&String) -> String,
        ) -> MetaResult<()> {
            let next_column_id = sink_columns
                .iter()
                .map(|col| col.column_desc.as_ref().unwrap().column_id + 1)
                .max()
                .unwrap_or(1);

            sink_columns.extend(new_columns.iter().enumerate().map(|(i, col)| {
                let mut col = col.to_protobuf();
                let column_desc = col.column_desc.as_mut().unwrap();
                column_desc.column_id = next_column_id + (i as i32);
                column_desc.name = get_column_name(&column_desc.name);
                col
            }));

            Ok(())
        }

        let PbNodeBody::Sink(sink_node_body) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected Sink node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        // Update sink desc columns and identity.
        let sink_desc = sink_node_body
            .sink_desc
            .as_mut()
            .ok_or_else(|| anyhow!("sink node must have sink_desc"))?;
        extend_sink_columns(&mut sink_desc.column_catalogs, added_columns, |name| {
            name.clone()
        })?;

        // following logic in <StreamSink as Explain>::distill
        node.identity = {
            let sink_type = SinkType::from_proto(ctx.sink.sink_type());
            let sink_type_str = sink_type.type_str();
            let column_names = sink_desc
                .column_catalogs
                .iter()
                .map(|col| {
                    ColumnCatalog::from(col.clone())
                        .name_with_hidden()
                        .to_string()
                })
                .join(", ");
            let downstream_pk = if !sink_type.is_append_only() {
                let downstream_pk = ctx
                    .sink
                    .downstream_pk
                    .iter()
                    .map(|i| {
                        &ctx.sink.columns[*i as usize]
                            .column_desc
                            .as_ref()
                            .unwrap()
                            .name
                    })
                    .collect_vec();
                format!(", downstream_pk: {downstream_pk:?}")
            } else {
                "".to_owned()
            };
            format!(
                "StreamSink {{ type: {sink_type_str}, columns: [{column_names}]{downstream_pk} }}"
            )
        };

        // Update plan fields.
        extend_pb_fields(&mut node.fields, added_columns, &ctx.upstream_table.name)?;

        // Update log store table columns if present.
        if let Some(log_store_table) = &mut sink_node_body.table {
            extend_sink_columns(&mut log_store_table.columns, added_columns, |name| {
                format!("{}_{}", ctx.upstream_table.name, name)
            })?;
        }

        Ok(())
    }
}

pub struct StreamScanRule;

impl OperatorSchemaChangeRule for StreamScanRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        let PbNodeBody::StreamScan(scan) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected StreamScan node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        // Update scan node fields + identity.
        extend_pb_fields(&mut node.fields, added_columns, &ctx.upstream_table.name)?;
        node.identity = {
            let columns = node.fields.iter().map(|col| &col.name).join(", ");
            format!("StreamTableScan {{ table: t, columns: [{columns}] }}")
        };

        // Update scan internals.
        scan.arrangement_table = Some(ctx.upstream_table.clone());
        scan.output_indices
            .extend((0..added_columns.len()).map(|i| (i + scan.upstream_column_ids.len()) as u32));
        scan.upstream_column_ids
            .extend(added_columns.iter().map(|col| col.column_id().get_id()));

        let table_desc = scan
            .table_desc
            .as_mut()
            .ok_or_else(|| anyhow!("stream scan must have table_desc"))?;
        table_desc
            .value_indices
            .extend((0..added_columns.len()).map(|i| (i + table_desc.columns.len()) as u32));
        table_desc.columns.extend(
            added_columns
                .iter()
                .map(|col| col.column_desc.to_protobuf()),
        );
        Ok(())
    }

    fn validate(&self, node: &StreamNode) -> MetaResult<()> {
        let PbNodeBody::StreamScan(scan) = node.node_body.as_ref().unwrap() else {
            return Err(anyhow!(
                "expected StreamScan node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let stream_scan_type = PbStreamScanType::try_from(scan.stream_scan_type).unwrap();
        if stream_scan_type != PbStreamScanType::ArrangementBackfill {
            return Err(anyhow!(
                "unsupported stream_scan_type for auto refresh schema: {:?}",
                stream_scan_type
            )
            .into());
        }

        let [merge_node, batch_plan_node] = node.input.as_slice() else {
            return Err(anyhow!(
                "unsupported StreamScan shape for auto refresh schema: expected 2 inputs, got {}",
                node.input.len()
            )
            .into());
        };

        let Some(PbNodeBody::Merge(_)) = merge_node.node_body.as_ref() else {
            return Err(anyhow!(
                "unsupported StreamScan child[0]: expected Merge, got {}",
                merge_node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };
        let Some(PbNodeBody::BatchPlan(_)) = batch_plan_node.node_body.as_ref() else {
            return Err(anyhow!(
                "unsupported StreamScan child[1]: expected BatchPlan, got {}",
                batch_plan_node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };
        Ok(())
    }
}

pub struct MergeRule;

impl OperatorSchemaChangeRule for MergeRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        let PbNodeBody::Merge(merge) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected Merge node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        // `upstream_fragment_id` exists in the mapping, meaning it should be a fragment inside the job.
        // Otherwise, it should connect to the upstream table fragment directly.
        let original_upstream_fragment_id = merge.upstream_fragment_id;
        if let Some(new_fragment_id) = ctx.fragment_id_mapping.get(&original_upstream_fragment_id) {
            merge.upstream_fragment_id = *new_fragment_id;
            extend_pb_fields(&mut node.fields, added_columns, &ctx.upstream_table.name)?;
        } else {
            merge.upstream_fragment_id = ctx.upstream_table_fragment_id;
            node.fields.extend(added_columns.iter().map(|col| {
                Field::new(col.column_desc.name.clone(), col.data_type().clone()).to_prost()
            }));
        };

        Ok(())
    }
}

pub struct BatchPlanRule;

impl OperatorSchemaChangeRule for BatchPlanRule {
    fn rewrite(
        &self,
        _node: &mut StreamNode,
        _change_set: &SinkSchemaChangeSet,
        _ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        // Intentionally left as a no-op for now.
        // For backfill variants other than ArrangementBackfill, `BatchPlan` may become
        // schema-relevant and should be handled here.
        Ok(())
    }
}

pub struct ProjectRule;

impl OperatorSchemaChangeRule for ProjectRule {
    fn rewrite(
        &self,
        node: &mut StreamNode,
        change_set: &SinkSchemaChangeSet,
        ctx: &RewriteContext<'_>,
    ) -> MetaResult<()> {
        let PbNodeBody::Project(project) = node.node_body.as_mut().unwrap() else {
            return Err(anyhow!(
                "expected Project node_body, got {}",
                node.node_body.as_ref().unwrap().discriminant()
            )
            .into());
        };

        let SinkSchemaChangeSet::AddColumns(added_columns) = change_set else {
            return Err(anyhow!("only support AddColumns in sink auto schema change").into());
        };

        let [input] = node.input.as_slice() else {
            return Err(anyhow!(
                "unsupported Project shape for auto refresh schema: expected 1 input, got {}",
                node.input.len()
            )
            .into());
        };

        if input.fields.len() < added_columns.len() {
            return Err(anyhow!(
                "unexpected Project input schema length for auto refresh schema: input_fields_len={}, added_columns_len={}",
                input.fields.len(),
                added_columns.len()
            )
            .into());
        }

        // Append passthrough expressions for newly added columns.
        let added_start = input.fields.len() - added_columns.len();
        project
            .select_list
            .extend(added_columns.iter().enumerate().map(|(i, col)| {
                let index = (added_start + i) as u32;
                PbExprNode {
                    function_type: risingwave_pb::expr::expr_node::Type::Unspecified as i32,
                    return_type: Some(col.data_type().to_protobuf()),
                    rex_node: Some(PbRexNode::InputRef(index)),
                }
            }));

        extend_pb_fields(&mut node.fields, added_columns, &ctx.upstream_table.name)?;

        node.identity = {
            let columns = node.fields.iter().map(|f| &f.name).join(", ");
            format!("StreamProject {{ exprs: [{columns}] }}")
        };

        Ok(())
    }
}
