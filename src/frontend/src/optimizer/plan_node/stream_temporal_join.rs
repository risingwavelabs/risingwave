// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::TemporalJoinNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_sqlparser::ast::AsOf;

use super::stream::prelude::*;
use super::utils::{Distill, childless_record, watermark_pretty};
use super::{ExprRewritable, PlanBase, PlanTreeNodeBinary, StreamPlanRef as PlanRef, generic};
use crate::TableCatalog;
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::plan_node::plan_tree_node::PlanTreeNodeUnary;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, StreamExchange, StreamTableScan, TryToStreamPb,
};
use crate::optimizer::property::WatermarkColumns;
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTemporalJoin {
    pub base: PlanBase<Stream>,
    core: generic::Join<PlanRef>,
    append_only: bool,
    is_nested_loop: bool,
    event_time: Option<EventTimeTemporalJoin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EventTimeTemporalJoin {
    left_event_time_idx: usize,
    right_event_time_idx: usize,
}

fn right_unique_key_table_indices(
    scan: &StreamTableScan,
    right_eq_indexes: &[usize],
    right_event_time_idx: usize,
) -> Vec<usize> {
    right_eq_indexes
        .iter()
        .copied()
        .chain(std::iter::once(right_event_time_idx))
        .map(|idx| scan.core().output_col_idx[idx])
        .unique()
        .collect_vec()
}

impl StreamTemporalJoin {
    pub fn new(core: generic::Join<PlanRef>, is_nested_loop: bool) -> Result<Self> {
        core.on
            .as_eq_predicate_ref()
            .expect("StreamTemporalJoin requires JoinOn::EqPredicate in core");
        assert!(core.join_type == JoinType::Inner || core.join_type == JoinType::LeftOuter);
        let stream_kind = reject_upsert_input!(core.left);
        let left_event_time_idx = core.temporal_event_time_as_of.as_ref().map(|as_of| {
            as_of
                .as_input_ref()
                .expect("event-time temporal join AS OF should be a left input ref")
                .index
        });
        let append_only = left_event_time_idx.is_some() || stream_kind.is_append_only();
        assert!(!is_nested_loop || append_only);

        let right = core.right.clone();
        let exchange: &StreamExchange = right
            .as_stream_exchange()
            .expect("temporal join right side should be a stream exchange");
        let exchange_input = exchange.input();
        let scan: &StreamTableScan = exchange_input
            .as_stream_table_scan()
            .expect("should be a stream table scan");
        let event_time = if let Some(left_event_time_idx) = left_event_time_idx {
            assert!(matches!(scan.core().as_of, Some(AsOf::EventTime(_))));
            assert!(!is_nested_loop, "event-time temporal join requires eq keys");
            if !core.left.watermark_columns().contains(left_event_time_idx) {
                bail!("event-time temporal join requires the AS OF column to have a watermark");
            }
            let right_watermark_indices = scan.core().watermark_columns().indices().collect_vec();
            if right_watermark_indices.len() != 1 {
                bail!(
                    "event-time temporal join requires the right versioned table to have exactly one watermark column"
                );
            }
            let right_event_time_idx = right_watermark_indices[0];
            if core.left.schema()[left_event_time_idx].data_type
                != scan.schema()[right_event_time_idx].data_type
            {
                bail!(
                    "event-time temporal join requires left AS OF column and right version time column to have the same type"
                );
            }
            let right_unique_key = self::right_unique_key_table_indices(
                scan,
                &core
                    .on
                    .as_eq_predicate_ref()
                    .expect("StreamTemporalJoin requires JoinOn::EqPredicate in core")
                    .right_eq_indexes(),
                right_event_time_idx,
            );
            if !scan
                .core()
                .primary_key()
                .iter()
                .all(|pk| right_unique_key.contains(&pk.column_index))
            {
                bail!(
                    "event-time temporal join requires the right join key and right version time column to cover the primary key of the right versioned table"
                );
            }
            Some(EventTimeTemporalJoin {
                left_event_time_idx,
                right_event_time_idx,
            })
        } else {
            assert!(
                exchange.no_shuffle(),
                "process-time temporal join right side should be a no shuffle stream exchange"
            );
            assert!(matches!(scan.core().as_of, Some(AsOf::ProcessTime)));
            None
        };

        let dist = if is_nested_loop {
            // Use right side distribution directly if it's nested loop temporal join.
            let r2o = core.r2i_col_mapping().composite(&core.i2o_col_mapping());
            r2o.rewrite_provided_distribution(core.right.distribution())
        } else {
            // Use left side distribution directly if it's hash temporal join.
            // https://github.com/risingwavelabs/risingwave/pull/19201#discussion_r1824031780
            let l2o = core.l2i_col_mapping().composite(&core.i2o_col_mapping());
            l2o.rewrite_provided_distribution(core.left.distribution())
        };

        let watermark_columns = if let Some(event_time) = &event_time {
            let mut watermark_columns = WatermarkColumns::new();
            if let Some(group) = core
                .left
                .watermark_columns()
                .get_group(event_time.left_event_time_idx)
            {
                let l2o = core.l2i_col_mapping().composite(&core.i2o_col_mapping());
                if let Some(output_idx) = l2o.try_map(event_time.left_event_time_idx) {
                    watermark_columns.insert(output_idx, group);
                }
            }
            watermark_columns
        } else {
            // Use left side watermark directly.
            core.left
                .watermark_columns()
                .map_clone(&core.l2i_col_mapping())
                .map_clone(&core.i2o_col_mapping())
        };

        let columns_monotonicity = core.i2o_col_mapping().rewrite_monotonicity_map(
            &core
                .l2i_col_mapping()
                .rewrite_monotonicity_map(core.left.columns_monotonicity()),
        );

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            stream_kind,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
            columns_monotonicity,
        );

        Ok(Self {
            base,
            core,
            append_only,
            is_nested_loop,
            event_time,
        })
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        self.core
            .on
            .as_eq_predicate_ref()
            .expect("StreamTemporalJoin should store predicate as EqJoinPredicate")
    }

    pub fn append_only(&self) -> bool {
        self.append_only
    }

    pub fn is_nested_loop(&self) -> bool {
        self.is_nested_loop
    }

    pub fn is_event_time(&self) -> bool {
        self.event_time.is_some()
    }

    /// Return memo-table catalog and its `pk_indices`.
    /// (`join_key` + `left_pk` + `right_pk`) -> (`right_scan_schema` + `join_key` + `left_pk`)
    ///
    /// Write pattern:
    ///   for each left input row (with insert op), construct the memo table pk and insert the row into the memo table.
    ///
    /// Read pattern:
    ///   for each left input row (with delete op), construct pk prefix (`join_key` + `left_pk`) to fetch rows and delete them from the memo table.
    pub fn infer_memo_table_catalog(&self, right_scan: &StreamTableScan) -> TableCatalog {
        let left_eq_indexes = self.eq_join_predicate().left_eq_indexes();
        let read_prefix_len_hint = left_eq_indexes.len() + self.left().stream_key().unwrap().len();

        // Build internal table
        let mut internal_table_catalog_builder = TableCatalogBuilder::default();
        // Add right table fields
        let right_scan_schema = right_scan.core().schema();
        for field in right_scan_schema.fields() {
            internal_table_catalog_builder.add_column(field);
        }
        // Add join_key + left_pk
        for field in left_eq_indexes
            .iter()
            .chain(self.core.left.stream_key().unwrap())
            .map(|idx| &self.core.left.schema().fields()[*idx])
        {
            internal_table_catalog_builder.add_column(field);
        }

        let mut pk_indices = vec![];
        pk_indices
            .extend(right_scan_schema.len()..(right_scan_schema.len() + read_prefix_len_hint));
        pk_indices.extend(right_scan.stream_key().unwrap());

        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
        });

        let dist_key_len = right_scan
            .core()
            .distribution_key()
            .map(|keys| keys.len())
            .unwrap_or(0);

        let internal_table_dist_keys =
            (right_scan_schema.len()..(right_scan_schema.len() + dist_key_len)).collect();
        internal_table_catalog_builder.build(internal_table_dist_keys, read_prefix_len_hint)
    }

    pub fn infer_event_time_table_catalogs(
        &self,
        right_scan: &StreamTableScan,
    ) -> (TableCatalog, TableCatalog, TableCatalog) {
        let event_time = self
            .event_time
            .as_ref()
            .expect("event-time table catalogs require event-time temporal join");
        let left_eq_indexes = self.eq_join_predicate().left_eq_indexes();
        let right_eq_indexes = self.eq_join_predicate().right_eq_indexes();
        let left = self.left();
        let left_stream_key = left.stream_key().unwrap();

        let mut left_time_table_builder = TableCatalogBuilder::default();
        for field in left.schema().fields() {
            left_time_table_builder.add_column(field);
        }
        left_time_table_builder
            .add_order_column(event_time.left_event_time_idx, OrderType::ascending());
        for idx in left_eq_indexes.iter().chain(left_stream_key.iter()) {
            left_time_table_builder.add_order_column(*idx, OrderType::ascending());
        }
        left_time_table_builder.set_clean_watermark_indices(vec![event_time.left_event_time_idx]);
        let left_time_table = left_time_table_builder.build(left_eq_indexes, 0);

        let mut right_key_table_builder = TableCatalogBuilder::default();
        for field in right_scan.schema().fields() {
            right_key_table_builder.add_column(field);
        }
        for idx in &right_eq_indexes {
            right_key_table_builder.add_order_column(*idx, OrderType::ascending());
        }
        right_key_table_builder
            .add_order_column(event_time.right_event_time_idx, OrderType::ascending());
        let right_key_table =
            right_key_table_builder.build(right_eq_indexes.clone(), right_eq_indexes.len());

        let mut right_time_table_builder = TableCatalogBuilder::default();
        let right_time_idx = right_time_table_builder
            .add_column(&right_scan.schema()[event_time.right_event_time_idx]);
        let right_time_join_key_indices = right_eq_indexes
            .iter()
            .map(|idx| right_time_table_builder.add_column(&right_scan.schema()[*idx]))
            .collect_vec();
        right_time_table_builder.add_order_column(right_time_idx, OrderType::ascending());
        for idx in &right_time_join_key_indices {
            right_time_table_builder.add_order_column(*idx, OrderType::ascending());
        }
        right_time_table_builder.set_clean_watermark_indices(vec![right_time_idx]);
        let right_time_table = right_time_table_builder.build(right_time_join_key_indices, 0);

        (left_time_table, right_key_table, right_time_table)
    }
}

impl Distill for StreamTemporalJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.core.join_type)));
        vec.push(("append_only", Pretty::debug(&self.append_only)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        vec.push(("nested_loop", Pretty::debug(&self.is_nested_loop)));

        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        childless_record("StreamTemporalJoin", vec)
    }
}

impl PlanTreeNodeBinary<Stream> for StreamTemporalJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = left;
        core.right = right;
        Self::new(core, self.is_nested_loop).unwrap()
    }
}

impl_plan_tree_node_for_binary! { Stream, StreamTemporalJoin }

impl TryToStreamPb for StreamTemporalJoin {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        let left_jk_indices = self.eq_join_predicate().left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate().right_eq_indexes();
        let left_jk_indices_prost = left_jk_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_jk_indices_prost = right_jk_indices.iter().map(|idx| *idx as i32).collect_vec();

        let null_safe_prost = self.eq_join_predicate().null_safes().into_iter().collect();

        let right = self.right();
        let exchange: &StreamExchange = right
            .as_stream_exchange()
            .expect("temporal join right side should be a stream exchange");
        if self.event_time.is_none() {
            assert!(
                exchange.no_shuffle(),
                "process-time temporal join right side should be a no shuffle stream exchange"
            );
        }
        let exchange_input = exchange.input();
        let scan: &StreamTableScan = exchange_input
            .as_stream_table_scan()
            .expect("should be a stream table scan");
        let event_time_tables = if self.event_time.is_some() {
            let (mut left_time, mut right_key, mut right_time) =
                self.infer_event_time_table_catalogs(scan);
            left_time = left_time.with_id(state.gen_table_id_wrapped());
            right_key = right_key.with_id(state.gen_table_id_wrapped());
            right_time = right_time.with_id(state.gen_table_id_wrapped());
            Some((left_time, right_key, right_time))
        } else {
            None
        };

        Ok(NodeBody::TemporalJoin(Box::new(TemporalJoinNode {
            join_type: self.core.join_type as i32,
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            condition: self
                .eq_join_predicate()
                .other_cond()
                .as_expr_unless_true()
                .map(|expr| {
                    expr.to_expr_proto_checked_pure(
                        self.left().stream_kind().is_retract()
                            || self.right().stream_kind().is_retract(),
                        "JOIN condition",
                    )
                })
                .transpose()?,
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            table_desc: Some(scan.core().table_catalog.table_desc().try_to_protobuf()?),
            table_output_indices: scan.core().output_col_idx.iter().map(|&i| i as _).collect(),
            memo_table: if self.append_only {
                None
            } else {
                let mut memo_table = self.infer_memo_table_catalog(scan);
                memo_table = memo_table.with_id(state.gen_table_id_wrapped());
                Some(memo_table.to_internal_table_prost())
            },
            is_nested_loop: self.is_nested_loop,
            left_event_time_key: self
                .event_time
                .as_ref()
                .map(|event_time| event_time.left_event_time_idx as u32),
            right_event_time_key: self
                .event_time
                .as_ref()
                .map(|event_time| event_time.right_event_time_idx as u32),
            event_time_left_time_table: event_time_tables
                .as_ref()
                .map(|tables| tables.0.to_internal_table_prost()),
            event_time_right_key_table: event_time_tables
                .as_ref()
                .map(|tables| tables.1.to_internal_table_prost()),
            event_time_right_time_table: event_time_tables
                .as_ref()
                .map(|tables| tables.2.to_internal_table_prost()),
        })))
    }
}

impl ExprRewritable<Stream> for StreamTemporalJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.is_nested_loop).unwrap().into()
    }
}

impl ExprVisitable for StreamTemporalJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
