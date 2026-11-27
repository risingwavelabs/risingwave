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
use std::sync::Arc;

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use crate::catalog::index_catalog::{IndexType, TableIndex};
use crate::catalog::table_catalog::TableType;
use crate::expr::ExprType;
use crate::optimizer::delta_join_solver::{
    DeltaJoinSolver, JoinEdge, JoinTable, LookupPathWithEdges, StreamStrategy,
};
use crate::optimizer::plan_node::generic::{self, GenericPlanRef, PhysicalPlanRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::{
    BackfillType, MultiwayDeltaJoinLookup, MultiwayDeltaJoinPath, PlanTreeNodeBinary,
    PlanTreeNodeUnary, Stream, StreamMultiwayDeltaJoin, StreamPlanRef as PlanRef, StreamTableScan,
};
use crate::optimizer::property::Distribution;
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::Condition;

/// Rewrites a tree of three or more inner equi-joins over table scans into a
/// [`StreamMultiwayDeltaJoin`], which joins the inputs by looking up their indexes without keeping
/// any join state.
///
/// The rule applies when:
///
/// * Every join in the tree is an inner join with only null-unsafe equal conditions. Filters of
///   `IS NOT NULL` on join keys, which the optimizer derives from the join conditions, are allowed,
///   because a lookup never matches a null join key.
/// * The equal conditions can be reduced to a tree of join edges, so that every lookup joins with
///   exactly one join input that has already been joined. Conditions implied by others through
///   transitivity are dropped. Join graphs that are still cyclic are left to the two-way
///   [`super::IndexDeltaJoinRule`].
/// * Every join input looked up on a join key has a full covering index, or a primary key, that is
///   distributed and ordered by that join key.
/// * The output of the join tree is hash distributed or not distributed in a known way. The output
///   of the delta join keeps the hash distribution, so the plan above it does not change.
pub struct MultiwayIndexDeltaJoinRule {}

/// An equal condition between two columns, as `(join input, column of the join input)`.
type EqCond = ((usize, usize), (usize, usize));

/// Join edges between pairs of join inputs `(a, b)` with `a < b`, with the join keys of `a` and `b`.
type JoinEdges = BTreeMap<(usize, usize), (Vec<usize>, Vec<usize>)>;

/// A tree of inner equi-joins, flattened into its join inputs.
struct FlattenedJoin {
    /// The join inputs, from left to right.
    inputs: Vec<StreamTableScan>,
    /// Every output column, as `(join input, column of the join input)`.
    columns: Vec<(usize, usize)>,
    /// The equal conditions.
    eq_conds: Vec<EqCond>,
    /// The columns filtered by `IS NOT NULL`, as `(join input, column of the join input)`.
    not_null: Vec<(usize, usize)>,
}

/// An index or the primary table of a join input.
enum ArrangementSource {
    Index(Arc<TableIndex>),
    PrimaryTable,
}

struct Arrangement {
    input: usize,
    source: ArrangementSource,
    /// The distribution key of the arrangement, as column indices of the join input.
    dist_key: Vec<usize>,
}

impl Rule<Stream> for MultiwayIndexDeltaJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        plan.as_stream_hash_join()?;
        let flattened = flatten(plan.clone())?;
        if flattened.inputs.len() < 3 {
            return None;
        }
        // The multi-way delta join does not derive watermarks yet.
        if !plan.watermark_columns().is_empty() {
            return None;
        }
        let stream_key = plan.stream_key()?.to_vec();
        let dist_key = match plan.distribution() {
            Distribution::HashShard(dist_key) => Some(dist_key.clone()),
            Distribution::SomeShard => None,
            _ => return None,
        };
        build_multiway_delta_join(&plan, flattened, stream_key, dist_key)
    }
}

impl MultiwayIndexDeltaJoinRule {
    pub fn create() -> BoxedRule<Stream> {
        Box::new(Self {})
    }
}

fn strip_exchange(mut plan: PlanRef) -> PlanRef {
    while let Some(exchange) = plan.as_stream_exchange() {
        plan = exchange.input();
    }
    plan
}

/// Returns a scan of the primary table with all its indexes, if `scan` reads an index table.
///
/// The streaming index selection may have replaced the scan of a join input with the scan of the
/// index distributed by the join key. Delta join may need other indexes of the same table, for
/// example when the table is joined on two different keys, so we go back to the primary table.
fn primary_table_scan(scan: &StreamTableScan) -> Option<StreamTableScan> {
    let core = scan.core();
    let index_table = &core.table_catalog;
    if index_table.table_type != TableType::Index || !core.predicate.always_true() {
        return None;
    }
    let session = scan.ctx().session_ctx().clone();
    let reader = session.env().catalog_reader().read_guard();
    let schema = reader
        .get_schema_by_id(index_table.database_id, index_table.schema_id)
        .ok()?;
    let index = schema
        .iter_index()
        .find(|index| index.index_table().id == index_table.id)?;
    let IndexType::Table(table_index) = &index.index_type else {
        return None;
    };
    let secondary_to_primary_mapping = table_index.secondary_to_primary_mapping();
    let output_col_idx = core
        .output_col_idx
        .iter()
        .map(|i| secondary_to_primary_mapping.get(i).copied())
        .collect::<Option<Vec<_>>>()?;
    let table_indexes = schema
        .get_created_indexes_by_table_id(index.primary_table.id)
        .into_iter()
        .filter_map(|index| match &index.index_type {
            IndexType::Table(table_index) => Some(table_index.clone()),
            IndexType::Vector(_) => None,
        })
        .collect();
    let primary_core = generic::TableScan::new(
        output_col_idx,
        index.primary_table.clone(),
        table_indexes,
        vec![],
        core.ctx.clone(),
        Condition::true_cond(),
        core.as_of.clone(),
    );
    Some(StreamTableScan::new_with_backfill_type(
        primary_core,
        scan.backfill_type(),
    ))
}

fn flatten(plan: PlanRef) -> Option<FlattenedJoin> {
    let plan = strip_exchange(plan);
    if let Some(scan) = plan.as_stream_table_scan() {
        let scan = primary_table_scan(scan).unwrap_or_else(|| scan.clone());
        return Some(FlattenedJoin {
            inputs: vec![scan.clone()],
            columns: (0..scan.schema().len()).map(|col| (0, col)).collect(),
            eq_conds: vec![],
            not_null: vec![],
        });
    }

    if let Some(filter) = plan.as_stream_filter() {
        let not_null = filter
            .predicate()
            .conjunctions
            .iter()
            .map(|conj| {
                let func_call = conj.as_function_call()?;
                if func_call.func_type() != ExprType::IsNotNull {
                    return None;
                }
                let [input] = func_call.inputs() else {
                    return None;
                };
                input.as_input_ref().map(|input_ref| input_ref.index())
            })
            .collect::<Option<Vec<_>>>()?;
        let mut flattened = flatten(filter.input())?;
        flattened
            .not_null
            .extend(not_null.into_iter().map(|col| flattened.columns[col]));
        return Some(flattened);
    }

    let join = plan.as_stream_hash_join()?;
    let predicate = join.eq_join_predicate();
    if join.join_type() != JoinType::Inner
        || predicate.has_non_eq()
        || predicate
            .null_safes()
            .into_iter()
            .any(|null_safe| null_safe)
    {
        return None;
    }

    let left = flatten(join.left())?;
    let right = flatten(join.right())?;
    let offset = left.inputs.len();
    let shift = |(input, col): (usize, usize)| (input + offset, col);

    let concat_columns = left
        .columns
        .iter()
        .copied()
        .chain(right.columns.iter().copied().map(shift))
        .collect_vec();
    let mut eq_conds = left.eq_conds;
    eq_conds.extend(
        right
            .eq_conds
            .into_iter()
            .map(|(l, r)| (shift(l), shift(r))),
    );
    for (l, r) in predicate.eq_indexes() {
        eq_conds.push((concat_columns[l], concat_columns[left.columns.len() + r]));
    }
    let columns = join
        .output_indices()
        .iter()
        .map(|&i| concat_columns[i])
        .collect();

    let mut not_null = left.not_null;
    not_null.extend(right.not_null.into_iter().map(shift));
    let mut inputs = left.inputs;
    inputs.extend(right.inputs);
    Some(FlattenedJoin {
        inputs,
        columns,
        eq_conds,
        not_null,
    })
}

/// Reduces the equal conditions to a tree of join edges between the join inputs.
///
/// The equal conditions divide the columns into classes of columns that are equal to each other.
/// The conditions of a class are equivalent to any set of conditions connecting all columns of the
/// class, so conditions implied by transitivity can be dropped. For each class, we connect its
/// columns one by one: to a column of a join input that already has an edge with the new one, or
/// otherwise to any column of a join input that is not yet connected with the new one. Returns
/// `None` if the conditions can not form a tree, for example `a.x = b.x and b.y = c.y and c.z =
/// a.z`, or if a class has two columns of the same join input.
///
/// Returns the edges as a map from `(a, b)` with `a < b` to the join keys of `a` and `b`.
fn join_edges(input_num: usize, eq_conds: &[EqCond]) -> Option<JoinEdges> {
    // Group the columns into classes of equal columns, in the order they appear.
    let mut classes: Vec<Vec<(usize, usize)>> = vec![];
    for &(l, r) in eq_conds {
        let l_class = classes.iter().position(|c| c.contains(&l));
        let r_class = classes.iter().position(|c| c.contains(&r));
        match (l_class, r_class) {
            (Some(i), Some(j)) if i == j => {}
            (Some(i), Some(j)) => {
                let (i, j) = (i.min(j), i.max(j));
                let merged = classes.remove(j);
                classes[i].extend(merged);
            }
            (Some(i), None) => classes[i].push(r),
            (None, Some(j)) => classes[j].push(l),
            (None, None) => classes.push(vec![l, r]),
        }
    }

    let mut edges = JoinEdges::new();
    // The connected component of each join input, through the edges added so far.
    let mut component = (0..input_num).collect_vec();
    for class in classes {
        if !class.iter().map(|(input, _)| input).all_unique() {
            // A condition on the columns of one join input can not be looked up.
            return None;
        }
        let mut connected = vec![class[0]];
        for &(input, col) in &class[1..] {
            let with_edge = connected
                .iter()
                .find(|(other, _)| edges.contains_key(&(input.min(*other), input.max(*other))));
            let target = with_edge.or_else(|| {
                connected
                    .iter()
                    .find(|(other, _)| component[*other] != component[input])
            });
            let &(other, other_col) = target?;
            let (a, a_col, b, b_col) = if input < other {
                (input, col, other, other_col)
            } else {
                (other, other_col, input, col)
            };
            let (a_key, b_key) = edges.entry((a, b)).or_default();
            a_key.push(a_col);
            b_key.push(b_col);
            let (from, to) = (component[input], component[other]);
            if from != to {
                for c in &mut component {
                    if *c == from {
                        *c = to;
                    }
                }
            }
            connected.push((input, col));
        }
    }
    // The join graph is a tree only if it is connected, which also makes every join input
    // reachable for the solver.
    if edges.len() != input_num - 1 || !component.iter().all_equal() {
        return None;
    }
    Some(edges)
}

/// Returns `perm` such that `key[perm[i]] == target[i]`, if `key` is a permutation of `target`.
fn permutation(key: &[usize], target: &[usize]) -> Option<Vec<usize>> {
    if key.len() != target.len() || !key.iter().all_unique() {
        return None;
    }
    target
        .iter()
        .map(|t| key.iter().position(|k| k == t))
        .collect()
}

/// Finds an arrangement of `scan` that can be looked up by `key` (column indices of `scan`). The
/// arrangement must be distributed by `key`, and its order key must start with the distribution
/// key. The arrangement must have every column of the scan, and its primary key must be in the
/// scan output, which the lookups need to maintain their copy of the arrangement. Returns the
/// arrangement and `perm`, where `key[perm[i]]` is the `i`-th column of the distribution key.
fn find_arrangement(
    scan: &StreamTableScan,
    key: &[usize],
) -> Option<(ArrangementSource, Vec<usize>)> {
    let core = scan.core();

    for index in &core.table_indexes {
        // Only full covering indexes can be used in delta join. `full_covering` only compares the
        // number of columns, which an expression index may match without having all columns.
        if !index.full_covering() || !core.is_covered_by(index.primary_to_secondary_mapping()) {
            continue;
        }
        let s2p_mapping = index.secondary_to_primary_mapping();
        let pk_in_output = index.index_table.pk.iter().all(|order| {
            s2p_mapping
                .get(&order.column_index)
                .is_some_and(|i| core.output_col_idx.contains(i))
        });
        if !pk_in_output {
            continue;
        }
        let p2s_mapping = index.primary_to_secondary_mapping();
        let Some(key_in_index) = key
            .iter()
            .map(|&i| p2s_mapping.get(&core.output_col_idx[i]).copied())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let dist_key = &index.index_table.distribution_key;
        let Some(perm) = permutation(&key_in_index, dist_key) else {
            continue;
        };
        let order_key_prefix = index
            .index_table
            .pk
            .iter()
            .map(|order| order.column_index)
            .take(dist_key.len())
            .collect_vec();
        if order_key_prefix != *dist_key {
            continue;
        }
        return Some((ArrangementSource::Index(index.clone()), perm));
    }

    // The primary table is also an arrangement.
    let dist_key = core.distribution_key()?;
    let perm = permutation(key, &dist_key)?;
    let table_dist_key = &core.table_catalog.distribution_key;
    let order_key_prefix = core
        .table_catalog
        .pk
        .iter()
        .map(|order| order.column_index)
        .take(table_dist_key.len())
        .collect_vec();
    if order_key_prefix != *table_dist_key
        || !core
            .table_catalog
            .pk
            .iter()
            .all(|order| core.output_col_idx.contains(&order.column_index))
    {
        return None;
    }
    Some((ArrangementSource::PrimaryTable, perm))
}

/// Builds the scan of an arrangement with the given backfill type.
fn build_arrangement_scan(
    scan: &StreamTableScan,
    source: &ArrangementSource,
    backfill_type: BackfillType,
) -> Option<StreamTableScan> {
    if scan.core().cross_database() {
        // A cross-database scan is always a cross-database snapshot backfill, which follows the
        // epochs of another database, while the lookups read the arrangements in the epochs of
        // this database.
        return None;
    }
    let arrangement = match source {
        ArrangementSource::Index(index) => scan.to_index_scan(
            index.index_table.clone(),
            index.primary_to_secondary_mapping(),
            index.function_mapping(),
            backfill_type,
        ),
        ArrangementSource::PrimaryTable => {
            StreamTableScan::new_with_backfill_type(scan.core().clone(), backfill_type)
        }
    };
    Some(arrangement)
}

fn build_multiway_delta_join(
    plan: &PlanRef,
    flattened: FlattenedJoin,
    stream_key: Vec<usize>,
    dist_key: Option<Vec<usize>>,
) -> Option<PlanRef> {
    let FlattenedJoin {
        inputs,
        columns,
        eq_conds,
        not_null,
    } = flattened;
    let input_num = inputs.len();

    // `IS NOT NULL` filters can be dropped only on join keys, which never match a null.
    if !not_null
        .iter()
        .all(|col| eq_conds.iter().any(|(l, r)| l == col || r == col))
    {
        return None;
    }

    let edges = join_edges(input_num, &eq_conds)?;

    // Only one join input is backfilled, and the lookups of its arrangement only see the rows the
    // backfill has emitted, by the primary key order of that arrangement. So the backfilled input
    // must be looked up through a single arrangement, which holds for a leaf of the join tree: it
    // has one join edge. Prefer the first join input.
    let degree = |input: usize| {
        edges
            .keys()
            .filter(|(a, b)| *a == input || *b == input)
            .count()
    };
    let backfilled = (0..input_num).find(|&input| degree(input) == 1)?;

    // The join inputs in the order of their paths: the path of the backfilled input comes first and
    // looks up the current epoch of all other join inputs, so that it produces the join result of
    // the snapshot. Each path looks up the current epoch of the join inputs after it, and the
    // previous epoch of the join inputs before it.
    let join_order = std::iter::once(backfilled)
        .chain((0..input_num).filter(|&input| input != backfilled))
        .collect_vec();
    let mut position = vec![0; input_num];
    for (pos, &input) in join_order.iter().enumerate() {
        position[input] = pos;
    }

    let edges = edges
        .into_iter()
        .map(|((a, b), (a_key, b_key))| JoinEdge {
            left: JoinTable(a),
            right: JoinTable(b),
            left_join_key: a_key,
            right_join_key: b_key,
        })
        .collect_vec();
    let solver = DeltaJoinSolver::new(
        StreamStrategy::LeftThisEpoch,
        edges,
        join_order.iter().copied().map(JoinTable).collect(),
    );
    let solved: Vec<LookupPathWithEdges> = solver.solve_with_edges().ok()?;

    let input_widths = inputs.iter().map(|s| s.schema().len()).collect_vec();
    let mut arrangements: Vec<Arrangement> = vec![];
    let mut paths = Vec::with_capacity(input_num);
    for solved_path in &solved {
        let stream_input = solved_path.stream.0;
        // The offset of each join input in the rows joined on this path.
        let mut offsets = BTreeMap::new();
        offsets.insert(stream_input, 0);
        let mut width = input_widths[stream_input];

        let mut lookups = Vec::with_capacity(solved_path.edges.len());
        for edge in &solved_path.edges {
            let (joined, looked_up) = (edge.left.0, edge.right.0);
            let (source, perm) = find_arrangement(&inputs[looked_up], &edge.right_join_key)?;
            let dist_key = perm.iter().map(|&i| edge.right_join_key[i]).collect_vec();
            let stream_key = perm
                .iter()
                .map(|&i| offsets[&joined] + edge.left_join_key[i])
                .collect_vec();
            let arrangement = match arrangements
                .iter()
                .position(|a| a.input == looked_up && a.dist_key == dist_key)
            {
                Some(arrangement) => arrangement,
                None => {
                    arrangements.push(Arrangement {
                        input: looked_up,
                        source,
                        dist_key: dist_key.clone(),
                    });
                    arrangements.len() - 1
                }
            };
            lookups.push(MultiwayDeltaJoinLookup {
                arrangement,
                stream_key,
                arrange_key: dist_key,
                use_current_epoch: position[looked_up] > position[stream_input],
            });
            offsets.insert(looked_up, width);
            width += input_widths[looked_up];
        }
        paths.push((stream_input, lookups));
    }

    // Every join input is looked up by the paths of the other join inputs, so it has at least one
    // arrangement. Its first arrangement provides the changes of its path.
    let stream_arrangements = (0..input_num)
        .map(|input| arrangements.iter().position(|a| a.input == input))
        .collect::<Option<Vec<_>>>()?;

    // Only the changes of the backfilled input need to be backfilled. All other arrangements only
    // need the upstream changes.
    if arrangements
        .iter()
        .filter(|a| a.input == backfilled)
        .count()
        != 1
    {
        return None;
    }
    let backfill_type = inputs[backfilled].backfill_type();
    if backfill_type != BackfillType::ArrangementBackfill {
        plan.ctx().session_ctx().notice_to_user(format!(
            "Delta join requires ArrangementBackfill on the backfilled side, but got {:?}. Fall back to regular join.",
            backfill_type
        ));
        return None;
    }
    let arrangement_scans = arrangements
        .iter()
        .enumerate()
        .map(|(i, arrangement)| {
            let backfill_type = if i == stream_arrangements[backfilled] {
                BackfillType::ArrangementBackfill
            } else {
                BackfillType::Replicated
            };
            let scan = build_arrangement_scan(
                &inputs[arrangement.input],
                &arrangement.source,
                backfill_type,
            )?;
            Some(PlanRef::from(scan))
        })
        .collect::<Option<Vec<_>>>()?;

    // Emit the path looking up only previous epochs first, so that each path produces the changes
    // from the result of the paths emitted before it.
    let paths = paths
        .into_iter()
        .rev()
        .map(|(stream_input, lookups)| MultiwayDeltaJoinPath {
            stream_arrangement: stream_arrangements[stream_input],
            lookups,
        })
        .collect_vec();

    let input_offsets = input_widths
        .iter()
        .scan(0, |offset, width| {
            let current = *offset;
            *offset += width;
            Some(current)
        })
        .collect_vec();
    let global = |(input, col): (usize, usize)| input_offsets[input] + col;
    let output_indices = columns.into_iter().map(global).collect();
    let eq_conds = eq_conds
        .into_iter()
        .map(|(l, r)| (global(l), global(r)))
        .collect();

    let join = StreamMultiwayDeltaJoin::new(
        arrangement_scans,
        arrangements.iter().map(|a| a.input).collect(),
        paths,
        eq_conds,
        output_indices,
        stream_key,
        dist_key,
    )
    .ok()?;
    Some(join.into())
}
