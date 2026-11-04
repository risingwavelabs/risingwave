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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::fulltext::{Analyzer, DEFAULT_ANALYZER, analyze_query};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;

use crate::catalog::index_catalog::FullTextIndex;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    BatchFilter, BatchFullTextSearch, BatchPlanRef, BatchProject, EqJoinPredicate, LogicalJoin,
    LogicalScan, ToBatch, generic,
};
use crate::optimizer::rule::IndexSelectionRule;
use crate::utils::Condition;

/// Uses a full-text index for a batch scan whose predicate has a top-level conjunct
/// `keyword_match(<indexed expression>, <constant query> [, <constant analyzer>])`.
///
/// ```text
/// LogicalScan { table: t, predicate: keyword_match(doc, 'q') AND other_cond }
/// ```
/// becomes
/// ```text
/// BatchProject (only if the scan requires more columns than it outputs)
/// └─BatchFilter { predicate: other_cond } (only if there is any)
///   └─BatchLookupJoin { table: t, predicate: stream key = primary key of t }
///     └─BatchFullTextSearch { index: idx, tokens: [q] }
/// ```
///
/// The index returns exactly the keys of rows matching `keyword_match` as of the query epoch, and
/// the lookup join reads the primary table at the same epoch, so `keyword_match` is not evaluated
/// again.
pub struct FullTextSearchRule;

struct ResolvedSearch<'a> {
    index: &'a Arc<FullTextIndex>,
    /// Position of the `keyword_match` conjunct answered by the index in the scan predicate.
    conjunct_idx: usize,
    query: String,
    tokens: Vec<String>,
    /// Positions of the primary table's primary key columns in the index key columns.
    pk_idx_in_key: Vec<usize>,
}

impl FullTextSearchRule {
    /// Returns the batch plan using a full-text index, or `None` if no index applies.
    pub fn to_batch(scan: &LogicalScan) -> Result<Option<BatchPlanRef>> {
        let Some(resolved) = Self::resolve(scan) else {
            return Ok(None);
        };
        Self::build(scan, resolved)
    }

    fn resolve(scan: &LogicalScan) -> Option<ResolvedSearch<'_>> {
        if scan.full_text_indexes().is_empty() {
            return None;
        }
        // The index only contains committed data, while a barrier read looks up the primary table
        // including uncommitted data. Rows missing from the index would then be silently missing
        // from the result.
        if scan.ctx().session_ctx().is_barrier_read() {
            return None;
        }
        for (conjunct_idx, conjunct) in scan.predicate().conjunctions.iter().enumerate() {
            let Some(call) = conjunct.as_function_call() else {
                continue;
            };
            if call.func_type() != ExprType::KeywordMatch {
                continue;
            }
            let (doc, query, analyzer) = match call.inputs() {
                [doc, query] => (doc, query, None),
                [doc, query, analyzer] => (doc, query, Some(analyzer)),
                _ => continue,
            };
            let Some(query) = const_varchar(query) else {
                continue;
            };
            let analyzer = match analyzer {
                None => Analyzer::from_name(DEFAULT_ANALYZER),
                Some(analyzer) => {
                    const_varchar(analyzer).and_then(|name| Analyzer::from_name(&name))
                }
            };
            // An unknown analyzer is reported by `keyword_match` at runtime.
            let Some(analyzer) = analyzer else {
                continue;
            };
            let tokens = analyze_query(analyzer, &query);
            // A query without tokens is an error of `keyword_match`, keep it as a scan so that the
            // error is raised.
            if tokens.is_empty() {
                continue;
            }
            for index in scan.full_text_indexes() {
                // The predicate of the scan refers to the columns of the table, as does the
                // indexed expression.
                if &index.indexed_expr != doc {
                    continue;
                }
                if index.info.analyzer != analyzer.name()
                    || index.info.analyzer_version != analyzer.version()
                {
                    continue;
                }
                let Some(pk_idx_in_key) = index.primary_key_idx_in_key_columns(scan.table()) else {
                    continue;
                };
                if pk_idx_in_key.is_empty() {
                    continue;
                }
                return Some(ResolvedSearch {
                    index,
                    conjunct_idx,
                    query,
                    tokens,
                    pk_idx_in_key,
                });
            }
        }
        None
    }

    fn build(scan: &LogicalScan, resolved: ResolvedSearch<'_>) -> Result<Option<BatchPlanRef>> {
        let remaining_predicate = Condition {
            conjunctions: (scan.predicate().conjunctions.iter().enumerate())
                .filter(|(idx, _)| *idx != resolved.conjunct_idx)
                .map(|(_, conjunct)| conjunct.clone())
                .collect(),
        };
        // Without the answered conjunct, the document column may no longer be required.
        let scan = &scan.clone_with_predicate(remaining_predicate);
        let ctx = scan.ctx();
        let table = scan.table();
        let as_of = scan.as_of();

        let search: BatchPlanRef = BatchFullTextSearch::new(
            ctx.clone(),
            resolved.index,
            table.id,
            resolved.query,
            resolved.tokens,
            as_of.clone(),
        )
        .into();
        let key_len = search.schema().len();

        // The predicate is rewritten to refer to the required columns of the scan.
        let (_, predicate, project_expr) = scan.predicate_pull_up();
        let required_col_idx = scan.required_col_idx();

        // Look up the required columns, plus the primary key columns for the join condition.
        let mut lookup_col_idx = required_col_idx.clone();
        let pk_idx_in_lookup = table
            .pk()
            .iter()
            .map(|order| {
                if let Some(pos) = lookup_col_idx
                    .iter()
                    .position(|idx| *idx == order.column_index)
                {
                    pos
                } else {
                    lookup_col_idx.push(order.column_index);
                    lookup_col_idx.len() - 1
                }
            })
            .collect_vec();
        let lookup_scan = LogicalScan::from(generic::TableScan::new(
            lookup_col_idx,
            table.clone(),
            vec![],
            vec![],
            ctx,
            Condition::true_cond(),
            as_of,
        ));
        let lookup_batch_scan = lookup_scan.to_batch()?;

        let on_condition = Condition {
            conjunctions: resolved
                .pk_idx_in_key
                .iter()
                .zip_eq_fast(pk_idx_in_lookup)
                .map(|(key_idx, lookup_idx)| {
                    IndexSelectionRule::create_null_safe_equal_expr(
                        *key_idx,
                        search.schema()[*key_idx].data_type(),
                        key_len + lookup_idx,
                        lookup_batch_scan.schema()[lookup_idx].data_type(),
                    )
                })
                .collect(),
        };
        let eq_predicate = EqJoinPredicate::create(
            key_len,
            lookup_batch_scan.schema().len(),
            on_condition.clone(),
        );
        let join = generic::Join::new(
            search,
            lookup_batch_scan,
            on_condition,
            JoinType::Inner,
            (key_len..key_len + required_col_idx.len()).collect(),
        );
        let Some(lookup_join) =
            LogicalJoin::gen_batch_lookup_join(&lookup_scan, eq_predicate, join, false)?
        else {
            return Ok(None);
        };

        let mut plan: BatchPlanRef = lookup_join.into();
        if !predicate.always_true() {
            plan = BatchFilter::new(generic::Filter::new(predicate, plan)).into();
        }
        if let Some(exprs) = project_expr {
            plan = BatchProject::new(generic::Project::new(exprs, plan)).into();
        }
        assert_eq!(plan.schema(), scan.schema());
        Ok(Some(plan))
    }
}

fn const_varchar(expr: &ExprImpl) -> Option<String> {
    match expr.try_fold_const()? {
        Ok(Some(ScalarImpl::Utf8(s))) => Some(s.into()),
        _ => None,
    }
}
