// Copyright 2025 RisingWave Labs
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

use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::stream::prelude::*;
use super::utils::TableCatalogBuilder;
use super::{
    ExprRewritable, ExprVisitable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, TryToStreamPb,
    generic,
};
use crate::TableCatalog;
use crate::binder::{DefineSlotKind, MeasureSlotKind};
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamMatchRecognize` implements [`super::Stream`] for a SQL `MATCH_RECOGNIZE` (row pattern
/// recognition) operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamMatchRecognize {
    pub base: PlanBase<Stream>,
    core: generic::MatchRecognize<PlanRef<Stream>>,
}

impl StreamMatchRecognize {
    pub fn new(core: generic::MatchRecognize<PlanRef<Stream>>) -> Self {
        // ONE ROW PER MATCH emits one row per completed match over append-only input, so the output
        // is append-only. The output schema is the partition-by columns followed by the measures, so
        // the partition key occupies the leading `n_part` output columns; the input was sharded by
        // the partition key (see `to_stream`), so the output is hash-sharded on those columns.
        let n_part = core.partition_by.len();
        let dist = Distribution::HashShard((0..n_part).collect());
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            StreamKind::AppendOnly,
            false,
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );
        Self { base, core }
    }

    /// Per-partition buffered-row state table. Layout:
    ///   `[ seq (i64) , <input columns..> ]`
    /// keyed by (partition columns, seq). The executor buffers the raw input row per live row and
    /// restores the buffer from here on recovery. `seq` is a per-partition monotonic id; consumed
    /// rows are deleted after each drain. DEFINE predicates and MEASURES are both evaluated at match
    /// time from the stored input rows, so neither is persisted. The partition columns and the order
    /// key are columns of the stored input row, so they are not stored separately; the partition
    /// columns serve as the key prefix.
    fn infer_state_table(&self) -> TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();
        let input_fields = self.core.input.schema().fields().to_vec();
        let partition_indices = self
            .core
            .partition_key_indices()
            .expect("partition keys validated to be columns");

        // seq
        tbl_builder.add_column(&Field::with_name(DataType::Int64, "seq"));
        // raw input columns (offset 1)
        for f in &input_fields {
            tbl_builder.add_column(f);
        }

        // pk: partition columns (within the stored input row, offset 1) then seq.
        let partition_positions: Vec<usize> = partition_indices.iter().map(|i| 1 + i).collect();
        for &p in &partition_positions {
            tbl_builder.add_order_column(p, OrderType::ascending());
        }
        tbl_builder.add_order_column(0, OrderType::ascending());
        // Distribute the state by the partition columns so each actor owns its partitions' state.
        // read_prefix_len_hint = 0: recovery does a full (empty-prefix) scan over the actor's vnodes,
        // so we must not assert a partition-length prefix on iteration.
        tbl_builder.build(partition_positions, 0)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamMatchRecognize {
    fn input(&self) -> PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamMatchRecognize }
impl_distill_by_unit!(StreamMatchRecognize, core, "StreamMatchRecognize");

impl TryToStreamPb for StreamMatchRecognize {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        use risingwave_pb::stream_plan::*;

        let retract = self.stream_kind().is_retract();

        // PARTITION BY / ORDER BY were validated to be plain columns in `to_stream`.
        let partition_by = self
            .core
            .partition_key_indices()
            .expect("partition keys validated to be columns")
            .into_iter()
            .map(|i| i as u32)
            .collect();
        let order_by = self
            .core
            .order_key_indices()
            .expect("order keys validated to be columns")
            .into_iter()
            .map(|i| i as u32)
            .collect();

        let measures = self
            .core
            .measures
            .iter()
            .map(|m| {
                let expr = m
                    .expr
                    .to_expr_proto_checked_pure(retract, "match_recognize measure")?;
                let slots = m
                    .slots
                    .iter()
                    .map(|s| MatchRecognizeMeasureSlot {
                        kind: match s.kind {
                            MeasureSlotKind::Last => 0,
                            MeasureSlotKind::First => 1,
                            MeasureSlotKind::Classifier => 2,
                            MeasureSlotKind::CountStar => 3,
                            MeasureSlotKind::Count => 4,
                            MeasureSlotKind::Min => 5,
                            MeasureSlotKind::Max => 6,
                            MeasureSlotKind::Sum => 7,
                        },
                        vars: s.vars.clone(),
                        col_idx: s.col_idx as u32,
                        data_type: Some(s.data_type.to_protobuf()),
                        agg_call: s.agg.as_ref().map(|a| a.to_protobuf()),
                    })
                    .collect();
                Ok(MatchRecognizeMeasure {
                    expr: Some(expr),
                    name: m.name.clone(),
                    slots,
                })
            })
            .collect::<crate::error::Result<Vec<_>>>()?;

        let defines = self
            .core
            .defines
            .iter()
            .map(|d| {
                let condition = d
                    .definition
                    .to_expr_proto_checked_pure(retract, "match_recognize define")?;
                let slots = d
                    .slots
                    .iter()
                    .map(|s| MatchRecognizeDefineSlot {
                        kind: match s.kind {
                            DefineSlotKind::SelfCol => 0,
                            DefineSlotKind::Prev => 1,
                            DefineSlotKind::Next => 2,
                            DefineSlotKind::RunningFirst => 3,
                            DefineSlotKind::RunningLast => 4,
                        },
                        vars: s.vars.clone(),
                        col_idx: s.col_idx as u32,
                        offset: s.offset as u32,
                    })
                    .collect();
                Ok(MatchRecognizeDefine {
                    symbol: d.symbol.clone(),
                    condition: Some(condition),
                    slots,
                })
            })
            .collect::<crate::error::Result<Vec<_>>>()?;

        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        Ok(NodeBody::MatchRecognize(MatchRecognizeNode {
            partition_by,
            order_by,
            measures,
            defines,
            pattern_node: Some(lower_pattern(&self.core.pattern)?),
            state_table: Some(state_table),
            after_match_skip: {
                use risingwave_sqlparser::ast::AfterMatchSkip;
                match &self.core.after_match_skip {
                    Some(AfterMatchSkip::ToNextRow) => "to_next_row".to_owned(),
                    Some(AfterMatchSkip::ToFirst(sym)) => format!("to_first:{}", sym.real_value()),
                    Some(AfterMatchSkip::ToLast(sym)) => format!("to_last:{}", sym.real_value()),
                    _ => "past_last_row".to_owned(),
                }
            },
            within: self
                .core
                .within
                .as_ref()
                .map(|w| w.to_expr_proto_checked_pure(retract, "match_recognize within"))
                .transpose()?,
        }))
    }
}

impl ExprRewritable<Stream> for StreamMatchRecognize {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef<Stream> {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for StreamMatchRecognize {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}

/// Lower a bound [`MatchRecognizePattern`] (the `sqlparser` AST) into the structured pattern proto
/// consumed by the executor. Parenthesized groups are flattened (the executor pattern has no group
/// node); anchors (`^`, `$`) and exclusions (`{- ... -}`) are rejected here as they are not yet
/// supported. This replaces the previous text round-trip (`Display` ↔ a hand-rolled parser).
fn lower_pattern(
    pattern: &risingwave_sqlparser::ast::MatchRecognizePattern,
) -> crate::error::Result<risingwave_pb::stream_plan::MatchRecognizePatternNode> {
    use risingwave_common::bail_not_implemented;
    use risingwave_pb::stream_plan::match_recognize_pattern_node::Node;
    use risingwave_pb::stream_plan::{
        MatchRecognizePatternNode, MatchRecognizePatternSeq, MatchRecognizePermutePattern,
        MatchRecognizeQuantifiedPattern,
    };
    use risingwave_sqlparser::ast::{MatchRecognizePattern as Pat, MatchRecognizeSymbol as Sym};

    fn named(symbol: &Sym) -> crate::error::Result<String> {
        match symbol {
            Sym::Named(ident) => Ok(ident.real_value()),
            Sym::Start | Sym::End => {
                bail_not_implemented!("row pattern anchors (^, $) in MATCH_RECOGNIZE")
            }
        }
    }

    fn node(n: Node) -> risingwave_pb::stream_plan::MatchRecognizePatternNode {
        MatchRecognizePatternNode { node: Some(n) }
    }

    fn lower_seq(
        patterns: &[Pat],
    ) -> crate::error::Result<risingwave_pb::stream_plan::MatchRecognizePatternSeq> {
        Ok(MatchRecognizePatternSeq {
            patterns: patterns
                .iter()
                .map(lower_pattern)
                .collect::<crate::error::Result<Vec<_>>>()?,
        })
    }

    match pattern {
        Pat::Symbol(symbol) => Ok(node(Node::Var(named(symbol)?))),
        Pat::Exclude(_) => {
            bail_not_implemented!("row pattern exclusions ({{- ... -}}) in MATCH_RECOGNIZE")
        }
        Pat::Permute(symbols) => {
            // PERMUTE expands to the alternation of all n! orderings of its variables, so the NFA
            // grows factorially. Cap the variable count to keep that bounded.
            const MAX_PERMUTE_VARS: usize = 6;
            if symbols.len() > MAX_PERMUTE_VARS {
                return Err(crate::error::ErrorCode::NotSupported(
                    format!(
                        "PERMUTE over {} variables (expands to {}! orderings)",
                        symbols.len(),
                        symbols.len()
                    ),
                    format!("PERMUTE supports at most {MAX_PERMUTE_VARS} variables"),
                )
                .into());
            }
            Ok(node(Node::Permute(MatchRecognizePermutePattern {
                vars: symbols
                    .iter()
                    .map(named)
                    .collect::<crate::error::Result<Vec<_>>>()?,
            })))
        }
        Pat::Concat(patterns) => Ok(node(Node::Concat(lower_seq(patterns)?))),
        Pat::Alternation(patterns) => Ok(node(Node::Alternation(lower_seq(patterns)?))),
        // A parenthesized group is purely syntactic grouping; flatten it away.
        Pat::Group(inner) => lower_pattern(inner),
        Pat::Repetition(inner, quantifier, reluctant) => {
            Ok(node(Node::Quantified(Box::new(MatchRecognizeQuantifiedPattern {
                inner: Some(Box::new(lower_pattern(inner)?)),
                quantifier: Some(lower_quantifier(quantifier)),
                reluctant: *reluctant,
            }))))
        }
    }
}

/// Map a [`RepetitionQuantifier`] to the proto quantifier. `*`, `+`, `?` map to their dedicated
/// kinds; the `{...}` forms all map to `RANGE` with an explicit `min` and an optional `max`.
fn lower_quantifier(
    quantifier: &risingwave_sqlparser::ast::RepetitionQuantifier,
) -> risingwave_pb::stream_plan::MatchRecognizeQuantifier {
    use risingwave_pb::stream_plan::MatchRecognizeQuantifier;
    use risingwave_pb::stream_plan::match_recognize_quantifier::Kind;
    use risingwave_sqlparser::ast::RepetitionQuantifier as Q;

    let (kind, min, max) = match quantifier {
        Q::ZeroOrMore => (Kind::Star, 0, None),
        Q::OneOrMore => (Kind::Plus, 0, None),
        Q::AtMostOne => (Kind::Question, 0, None),
        Q::Exactly(n) => (Kind::Range, *n, Some(*n)),
        Q::AtLeast(n) => (Kind::Range, *n, None),
        Q::AtMost(m) => (Kind::Range, 0, Some(*m)),
        Q::Range(n, m) => (Kind::Range, *n, Some(*m)),
    };
    MatchRecognizeQuantifier {
        kind: kind as i32,
        min,
        max,
    }
}
