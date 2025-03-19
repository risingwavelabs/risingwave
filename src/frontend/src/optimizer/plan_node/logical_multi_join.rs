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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Schema;
use risingwave_pb::plan_common::JoinType;

use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalJoin, LogicalProject, PlanBase,
    PlanNodeType, PlanRef, PlanTreeNodeBinary, PlanTreeNodeUnary, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PlanTreeNode, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{
    ColIndexMapping, ColIndexMappingRewriteExt, Condition, ConditionDisplay,
    ConnectedComponentLabeller,
};

/// `LogicalMultiJoin` combines two or more relations according to some condition.
///
/// Each output row has fields from one the inputs. The set of output rows is a subset
/// of the cartesian product of all the inputs; The `LogicalMultiInnerJoin` is only supported
/// for inner joins as it implicitly assumes commutativity. Non-inner joins should be
/// expressed as 2-way `LogicalJoin`s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalMultiJoin {
    pub base: PlanBase<Logical>,
    inputs: Vec<PlanRef>,
    on: Condition,
    output_indices: Vec<usize>,
    inner2output: ColIndexMapping,
    // NOTE(st1page): these fields will be used in prune_col and
    // pk_derive soon.
    /// the mapping `output_col_idx` -> (`input_idx`, `input_col_idx`), **"`output_col_idx`" is internal,
    /// not consider `output_indices`**
    inner_o2i_mapping: Vec<(usize, usize)>,
    inner_i2o_mappings: Vec<ColIndexMapping>,
}

impl Distill for LogicalMultiJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = (self.inputs.iter())
            .flat_map(|input| input.schema().fields.clone())
            .collect();
        let input_schema = Schema { fields };
        let cond = Pretty::display(&ConditionDisplay {
            condition: self.on(),
            input_schema: &input_schema,
        });
        childless_record("LogicalMultiJoin", vec![("on", cond)])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalMultiJoinBuilder {
    output_indices: Vec<usize>,
    /// the predicates in the on condition, we do not use Condition here to emit unnecessary
    /// simplify.
    conjunctions: Vec<ExprImpl>,
    inputs: Vec<PlanRef>,
    tot_input_col_num: usize,
}

impl LogicalMultiJoinBuilder {
    /// add a predicate above the plan, so they will be rewritten from the `output_indices` to the
    /// input indices
    pub fn add_predicate_above(&mut self, exprs: impl Iterator<Item = ExprImpl>) {
        let mut mapping = ColIndexMapping::new(
            self.output_indices.iter().map(|i| Some(*i)).collect(),
            self.tot_input_col_num,
        );
        self.conjunctions
            .extend(exprs.map(|expr| mapping.rewrite_expr(expr)));
    }

    pub fn build(self) -> LogicalMultiJoin {
        LogicalMultiJoin::new(
            self.inputs,
            Condition {
                conjunctions: self.conjunctions,
            },
            self.output_indices,
        )
    }

    pub fn into_parts(self) -> (Vec<usize>, Vec<ExprImpl>, Vec<PlanRef>, usize) {
        (
            self.output_indices,
            self.conjunctions,
            self.inputs,
            self.tot_input_col_num,
        )
    }

    pub fn new(plan: PlanRef) -> LogicalMultiJoinBuilder {
        match plan.node_type() {
            PlanNodeType::LogicalJoin => Self::with_join(plan),
            PlanNodeType::LogicalFilter => Self::with_filter(plan),
            PlanNodeType::LogicalProject => Self::with_project(plan),
            _ => Self::with_input(plan),
        }
    }

    fn with_join(plan: PlanRef) -> LogicalMultiJoinBuilder {
        let join: &LogicalJoin = plan.as_logical_join().unwrap();
        if join.join_type() != JoinType::Inner {
            return Self::with_input(plan);
        }
        let left = join.left();
        let right = join.right();

        let mut builder = Self::new(left);

        let (r_output_indices, r_conjunctions, mut r_inputs, r_tot_input_col_num) =
            Self::new(right).into_parts();

        // the mapping from the right's column index to the current multi join's internal column
        // index
        let mut shift_mapping = ColIndexMapping::with_shift_offset(
            r_tot_input_col_num,
            builder.tot_input_col_num as isize,
        );
        builder.inputs.append(&mut r_inputs);
        builder.tot_input_col_num += r_tot_input_col_num;

        builder.conjunctions.extend(
            r_conjunctions
                .into_iter()
                .map(|expr| shift_mapping.rewrite_expr(expr)),
        );

        builder.output_indices.extend(
            r_output_indices
                .into_iter()
                .map(|idx| shift_mapping.map(idx)),
        );
        builder.add_predicate_above(join.on().conjunctions.iter().cloned());

        builder.output_indices = join
            .output_indices()
            .iter()
            .map(|idx| builder.output_indices[*idx])
            .collect();
        builder
    }

    fn with_filter(plan: PlanRef) -> LogicalMultiJoinBuilder {
        let filter: &LogicalFilter = plan.as_logical_filter().unwrap();
        let mut builder = Self::new(filter.input());
        builder.add_predicate_above(filter.predicate().conjunctions.iter().cloned());
        builder
    }

    fn with_project(plan: PlanRef) -> LogicalMultiJoinBuilder {
        let proj: &LogicalProject = plan.as_logical_project().unwrap();
        let output_indices = match proj.try_as_projection() {
            Some(output_indices) => output_indices,
            None => return Self::with_input(plan),
        };
        let mut builder = Self::new(proj.input());
        builder.output_indices = output_indices
            .into_iter()
            .map(|i| builder.output_indices[i])
            .collect();
        builder
    }

    fn with_input(input: PlanRef) -> LogicalMultiJoinBuilder {
        LogicalMultiJoinBuilder {
            output_indices: (0..input.schema().len()).collect_vec(),
            conjunctions: vec![],
            tot_input_col_num: input.schema().len(),
            inputs: vec![input],
        }
    }

    pub fn inputs(&self) -> &[PlanRef] {
        self.inputs.as_ref()
    }
}
impl LogicalMultiJoin {
    pub(crate) fn new(inputs: Vec<PlanRef>, on: Condition, output_indices: Vec<usize>) -> Self {
        let input_schemas = inputs
            .iter()
            .map(|input| input.schema().clone())
            .collect_vec();

        let (inner_o2i_mapping, tot_col_num) = {
            let mut inner_o2i_mapping = vec![];
            let mut tot_col_num = 0;
            for (input_idx, input_schema) in input_schemas.iter().enumerate() {
                tot_col_num += input_schema.len();
                for (col_idx, _field) in input_schema.fields().iter().enumerate() {
                    inner_o2i_mapping.push((input_idx, col_idx));
                }
            }
            (inner_o2i_mapping, tot_col_num)
        };
        let inner2output = ColIndexMapping::with_remaining_columns(&output_indices, tot_col_num);

        let schema = Schema {
            fields: output_indices
                .iter()
                .map(|idx| inner_o2i_mapping[*idx])
                .map(|(input_idx, col_idx)| input_schemas[input_idx].fields()[col_idx].clone())
                .collect(),
        };

        let inner_i2o_mappings = {
            let mut i2o_maps = vec![];
            for input_schema in &input_schemas {
                let map = vec![None; input_schema.len()];
                i2o_maps.push(map);
            }
            for (out_idx, (input_idx, in_idx)) in inner_o2i_mapping.iter().enumerate() {
                i2o_maps[*input_idx][*in_idx] = Some(out_idx);
            }

            i2o_maps
                .into_iter()
                .map(|map| ColIndexMapping::new(map, tot_col_num))
                .collect_vec()
        };

        let pk_indices = Self::derive_stream_key(&inputs, &inner_i2o_mappings, &inner2output);
        let functional_dependency = {
            let mut fd_set = FunctionalDependencySet::new(tot_col_num);
            let mut column_cnt: usize = 0;
            let id_mapping = ColIndexMapping::identity(tot_col_num);
            for i in &inputs {
                let mapping =
                    ColIndexMapping::with_shift_offset(i.schema().len(), column_cnt as isize)
                        .composite(&id_mapping);
                mapping
                    .rewrite_functional_dependency_set(i.functional_dependency().clone())
                    .into_dependencies()
                    .into_iter()
                    .for_each(|fd| fd_set.add_functional_dependency(fd));
                column_cnt += i.schema().len();
            }
            for i in &on.conjunctions {
                if let Some((col, _)) = i.as_eq_const() {
                    fd_set.add_constant_columns(&[col.index()])
                } else if let Some((left, right)) = i.as_eq_cond() {
                    fd_set.add_functional_dependency_by_column_indices(
                        &[left.index()],
                        &[right.index()],
                    );
                    fd_set.add_functional_dependency_by_column_indices(
                        &[right.index()],
                        &[left.index()],
                    );
                }
            }
            ColIndexMapping::with_remaining_columns(&output_indices, tot_col_num)
                .rewrite_functional_dependency_set(fd_set)
        };
        let base =
            PlanBase::new_logical(inputs[0].ctx(), schema, pk_indices, functional_dependency);

        Self {
            base,
            inputs,
            on,
            output_indices,
            inner2output,
            inner_o2i_mapping,
            inner_i2o_mappings,
        }
    }

    fn derive_stream_key(
        inputs: &[PlanRef],
        inner_i2o_mappings: &[ColIndexMapping],
        inner2output: &ColIndexMapping,
    ) -> Option<Vec<usize>> {
        // TODO(st1page): add JOIN key
        let mut pk_indices = vec![];
        for (i, input) in inputs.iter().enumerate() {
            let input_stream_key = input.stream_key()?;
            for input_pk_idx in input_stream_key {
                pk_indices.push(inner_i2o_mappings[i].map(*input_pk_idx));
            }
        }
        pk_indices
            .into_iter()
            .map(|col_idx| inner2output.try_map(col_idx))
            .collect::<Option<Vec<_>>>()
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new(self.inputs.clone(), cond, self.output_indices.clone())
    }
}

impl PlanTreeNode for LogicalMultiJoin {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(
            inputs.to_vec(),
            self.on().clone(),
            self.output_indices.clone(),
        )
        .into()
    }
}

impl LogicalMultiJoin {
    pub fn as_reordered_left_deep_join(&self, join_ordering: &[usize]) -> PlanRef {
        assert_eq!(join_ordering.len(), self.inputs.len());
        assert!(!join_ordering.is_empty());

        let base_plan = self.inputs[join_ordering[0]].clone();

        // Express as a cross join, we will rely on filter pushdown to push all of the join
        // conditions to convert into inner joins.
        let mut output = join_ordering[1..]
            .iter()
            .fold(base_plan, |join_chain, &index| {
                LogicalJoin::new(
                    join_chain,
                    self.inputs[index].clone(),
                    JoinType::Inner,
                    Condition::true_cond(),
                )
                .into()
            });

        let total_col_num = self.inner2output.source_size();
        let reorder_mapping = {
            let mut reorder_mapping = vec![None; total_col_num];
            join_ordering
                .iter()
                .cloned()
                .flat_map(|input_idx| {
                    (0..self.inputs[input_idx].schema().len())
                        .map(move |col_idx| self.inner_i2o_mappings[input_idx].map(col_idx))
                })
                .enumerate()
                .for_each(|(tar, src)| reorder_mapping[src] = Some(tar));
            reorder_mapping
        };
        output =
            LogicalProject::with_out_col_idx(output, reorder_mapping.iter().map(|i| i.unwrap()))
                .into();

        // We will later push down all of the filters back to the individual joins via the
        // `FilterJoinRule`.
        output = LogicalFilter::create(output, self.on.clone());
        output =
            LogicalProject::with_out_col_idx(output, self.output_indices.iter().cloned()).into();

        output
    }

    #[allow(clippy::doc_overindented_list_items)]
    /// Our heuristic join reordering algorithm will try to perform a left-deep join.
    /// It will try to do the following:
    ///
    /// 1. First, split the join graph, with eq join conditions as graph edges, into their connected
    ///    components. Repeat the procedure in 2. with the largest connected components down to
    ///    the smallest.
    ///
    /// 2. For each connected component, add joins to the chain, prioritizing adding those
    ///    joins to the bottom of the chain if their join conditions have:
    ///
    ///      a. eq joins between primary keys on both sides
    ///      b. eq joins with primary keys on one side
    ///      c. more equijoin conditions
    ///
    ///    in that order. This forms our selectivity heuristic.
    ///
    /// 3. Thirdly, we will emit a left-deep cross-join of each of the left-deep joins of the
    ///    connected components. Depending on the type of plan, this may result in a planner failure
    ///    (e.g. for streaming). No cross-join will be emitted for a single connected component.
    ///
    /// 4. Finally, we will emit, above the left-deep join tree:
    ///    a. a filter with the non eq conditions
    ///    b. a projection which reorders the output column ordering to agree with the original ordering of the joins.
    ///    The filter will then be pushed down by another filter pushdown pass.
    pub(crate) fn heuristic_ordering(&self) -> Result<Vec<usize>> {
        let mut labeller = ConnectedComponentLabeller::new(self.inputs.len());

        let (eq_join_conditions, _) = self.on.clone().split_by_input_col_nums(
            &self.input_col_nums(),
            // only_eq=
            true,
        );

        // Iterate over all join conditions, whose keys represent edges on the join graph
        for k in eq_join_conditions.keys() {
            labeller.add_edge(k.0, k.1);
        }

        let mut edge_sets: Vec<_> = labeller.into_edge_sets();

        // Sort in decreasing order of len
        edge_sets.sort_by_key(|a| std::cmp::Reverse(a.len()));

        let mut join_ordering = vec![];

        for component in edge_sets {
            let mut eq_cond_edges: Vec<(usize, usize)> = component.into_iter().collect();

            // TODO(jon-chuang): add sorting of eq_cond_edges based on selectivity here
            eq_cond_edges.sort();

            if eq_cond_edges.is_empty() {
                // There is nothing to join in this connected component
                break;
            };

            let edge = eq_cond_edges.remove(0);
            join_ordering.extend(&vec![edge.0, edge.1]);

            while !eq_cond_edges.is_empty() {
                let mut found = vec![];
                for (idx, edge) in eq_cond_edges.iter().enumerate() {
                    // If the eq join condition is on the existing join, we don't add any new
                    // inputs to the join
                    if join_ordering.contains(&edge.1) && join_ordering.contains(&edge.0) {
                        found.push(idx);
                    } else {
                        // Else, the eq join condition involves a new input, or is not connected to
                        // the existing left deep tree. Handle accordingly.
                        let new_input = if join_ordering.contains(&edge.0) {
                            edge.1
                        } else if join_ordering.contains(&edge.1) {
                            edge.0
                        } else {
                            continue;
                        };
                        join_ordering.push(new_input);
                        found.push(idx);
                    }
                }
                // This ensures eq_cond_edges.len() is strictly decreasing per iteration
                // Since the graph is connected, it is always possible to find at least one edge
                // remaining that can be connected to the current join result.
                if found.is_empty() {
                    return Err(RwError::from(ErrorCode::InternalError(
                        "Connecting edge not found in join connected subgraph".into(),
                    )));
                }
                let mut idx = 0;
                eq_cond_edges.retain(|_| {
                    let keep = !found.contains(&idx);
                    idx += 1;
                    keep
                });
            }
        }
        // Deal with singleton inputs (with no eq condition joins between them whatsoever)
        for i in 0..self.inputs.len() {
            if !join_ordering.contains(&i) {
                join_ordering.push(i);
            }
        }
        Ok(join_ordering)
    }

    #[allow(clippy::doc_overindented_list_items)]
    /// transform multijoin into bushy tree join.
    ///
    /// 1. First, use equivalent condition derivation to get derive join relation.
    /// 2. Second, for every isolated node will create connection to every other nodes.
    /// 3. Third, select and merge one node for a iteration, and use a bfs policy for which node the
    ///    selected node merged with.
    ///    i. The select node mentioned above is the node with least number of relations and the lowerst join tree.
    ///    ii. nodes with a join tree higher than the temporal optimal join tree will be pruned.
    pub fn as_bushy_tree_join(&self) -> Result<PlanRef> {
        let (nodes, condition) = self.get_join_graph()?;

        if nodes.is_empty() {
            return Err(RwError::from(ErrorCode::InternalError(
                "empty multi-join graph".into(),
            )));
        }

        let mut optimized_bushy_tree: Option<(GraphNode, Vec<GraphNode>)> = None;
        let mut que: VecDeque<(BTreeMap<usize, GraphNode>, Vec<GraphNode>)> =
            VecDeque::from([(nodes, vec![])]);

        while let Some((mut nodes, mut isolated)) = que.pop_front() {
            if nodes.len() == 1 {
                let node = nodes.into_values().next().unwrap();

                if let Some((old, _)) = &optimized_bushy_tree {
                    if node.join_tree.height < old.join_tree.height {
                        optimized_bushy_tree = Some((node, isolated));
                    }
                } else {
                    optimized_bushy_tree = Some((node, isolated));
                }
                continue;
            } else if nodes.is_empty() {
                if optimized_bushy_tree.is_none() {
                    let base = isolated.pop().unwrap();
                    optimized_bushy_tree = Some((base, isolated));
                }
                continue;
            }

            let (idx, _) = nodes
                .iter()
                .min_by(
                    |(_, x), (_, y)| match x.relations.len().cmp(&y.relations.len()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Equal => x.join_tree.height.cmp(&y.join_tree.height),
                    },
                )
                .unwrap();
            let n_id = *idx;

            let n = nodes.get(&n_id).unwrap();
            if n.relations.is_empty() {
                let n = nodes.remove(&n_id).unwrap();
                isolated.push(n);
                que.push_back((nodes, isolated));
                continue;
            }

            let mut relations = nodes
                .get_mut(&n_id)
                .unwrap()
                .relations
                .iter()
                .cloned()
                .collect_vec();
            relations.sort_by(|a, b| {
                let a = nodes.get(a).unwrap();
                let b = nodes.get(b).unwrap();
                match a.join_tree.height.cmp(&b.join_tree.height) {
                    Ordering::Equal => a.id.cmp(&b.id),
                    other => other,
                }
            });

            for merge_node_id in &relations {
                let mut nodes = nodes.clone();
                let n = nodes.remove(&n_id).unwrap();

                for adj_node_id in &n.relations {
                    if adj_node_id != merge_node_id {
                        let adj_node = nodes.get_mut(adj_node_id).unwrap();
                        adj_node.relations.remove(&n_id);
                        adj_node.relations.insert(*merge_node_id);
                        let merge_node = nodes.get_mut(merge_node_id).unwrap();
                        merge_node.relations.insert(*adj_node_id);
                    }
                }

                let merge_node = nodes.get_mut(merge_node_id).unwrap();
                merge_node.relations.remove(&n_id);
                let l_tree = n.join_tree.clone();
                let r_tree = std::mem::take(&mut merge_node.join_tree);
                let new_height = usize::max(l_tree.height, r_tree.height) + 1;

                if let Some(min_height) = optimized_bushy_tree
                    .as_ref()
                    .map(|(t, _)| t.join_tree.height)
                    && min_height < new_height
                {
                    continue;
                }

                merge_node.join_tree = JoinTreeNode {
                    idx: None,
                    left: Some(Box::new(l_tree)),
                    right: Some(Box::new(r_tree)),
                    height: new_height,
                };

                que.push_back((nodes, isolated.clone()));
            }
        }

        // maintain join order to mapping columns.
        let mut join_ordering = vec![];
        let mut output = if let Some((optimized_bushy_tree, isolated)) = optimized_bushy_tree {
            let optimized_bushy_tree =
                isolated
                    .into_iter()
                    .fold(optimized_bushy_tree, |chain, n| GraphNode {
                        id: n.id,
                        relations: BTreeSet::default(),
                        join_tree: JoinTreeNode {
                            height: chain.join_tree.height.max(n.join_tree.height) + 1,
                            idx: None,
                            left: Some(Box::new(chain.join_tree)),
                            right: Some(Box::new(n.join_tree)),
                        },
                    });
            self.create_logical_join(optimized_bushy_tree.join_tree, &mut join_ordering)?
        } else {
            return Err(RwError::from(ErrorCode::InternalError(
                "no plan remain".into(),
            )));
        };

        let total_col_num = self.inner2output.source_size();
        let reorder_mapping = {
            let mut reorder_mapping = vec![None; total_col_num];

            join_ordering
                .iter()
                .cloned()
                .flat_map(|input_idx| {
                    (0..self.inputs[input_idx].schema().len())
                        .map(move |col_idx| self.inner_i2o_mappings[input_idx].map(col_idx))
                })
                .enumerate()
                .for_each(|(tar, src)| reorder_mapping[src] = Some(tar));
            reorder_mapping
        };
        output =
            LogicalProject::with_out_col_idx(output, reorder_mapping.iter().map(|i| i.unwrap()))
                .into();

        output = LogicalFilter::create(output, condition);
        output =
            LogicalProject::with_out_col_idx(output, self.output_indices.iter().cloned()).into();
        Ok(output)
    }

    pub(crate) fn input_col_nums(&self) -> Vec<usize> {
        self.inputs.iter().map(|i| i.schema().len()).collect()
    }

    /// get join graph from `self.on`, return the join graph and the new join condition.
    fn get_join_graph(&self) -> Result<(BTreeMap<usize, GraphNode>, Condition)> {
        let mut nodes: BTreeMap<_, _> = (0..self.inputs.len())
            .map(|idx| GraphNode {
                id: idx,
                relations: BTreeSet::default(),
                join_tree: JoinTreeNode {
                    idx: Some(idx),
                    left: None,
                    right: None,
                    height: 0,
                },
            })
            .enumerate()
            .collect();

        let condition = self.on.clone();
        let condition = self.eq_condition_derivation(condition)?;
        let (eq_join_conditions, _) = condition
            .clone()
            .split_by_input_col_nums(&self.input_col_nums(), true);

        for ((src, dst), _) in eq_join_conditions {
            nodes.get_mut(&src).unwrap().relations.insert(dst);
            nodes.get_mut(&dst).unwrap().relations.insert(src);
        }

        Ok((nodes, condition))
    }

    ///  equivalent condition derivation by `a = b && a = c` ==> `b = c`
    fn eq_condition_derivation(&self, mut condition: Condition) -> Result<Condition> {
        let (eq_join_conditions, _) = condition
            .clone()
            .split_by_input_col_nums(&self.input_col_nums(), true);

        let mut new_conj: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        let mut input_ref_map = BTreeMap::new();

        for con in eq_join_conditions.values() {
            for conj in &con.conjunctions {
                let (l, r) = conj.as_eq_cond().unwrap();
                new_conj.entry(l.index).or_default().insert(r.index);
                new_conj.entry(r.index).or_default().insert(l.index);
                input_ref_map.insert(l.index, Some(l));
                input_ref_map.insert(r.index, Some(r));
            }
        }

        let mut new_pairs = BTreeSet::new();

        for conjs in new_conj.values() {
            if conjs.len() < 2 {
                continue;
            }

            let conjs = conjs.iter().copied().collect_vec();
            for i in 0..conjs.len() {
                for j in i + 1..conjs.len() {
                    if !new_conj.get(&conjs[i]).unwrap().contains(&conjs[j]) {
                        if conjs[i] < conjs[j] {
                            new_pairs.insert((conjs[i], conjs[j]));
                        } else {
                            new_pairs.insert((conjs[j], conjs[i]));
                        }
                    }
                }
            }
        }
        for (i, j) in new_pairs {
            condition
                .conjunctions
                .push(ExprImpl::FunctionCall(Box::new(FunctionCall::new(
                    ExprType::Equal,
                    vec![
                        ExprImpl::InputRef(Box::new(
                            input_ref_map.get(&i).unwrap().as_ref().unwrap().clone(),
                        )),
                        ExprImpl::InputRef(Box::new(
                            input_ref_map.get(&j).unwrap().as_ref().unwrap().clone(),
                        )),
                    ],
                )?)));
        }
        Ok(condition)
    }

    /// create logical plan by recursively travase `JoinTreeNode`
    fn create_logical_join(
        &self,
        mut join_tree: JoinTreeNode,
        join_ordering: &mut Vec<usize>,
    ) -> Result<PlanRef> {
        Ok(match (join_tree.left.take(), join_tree.right.take()) {
            (Some(l), Some(r)) => LogicalJoin::new(
                self.create_logical_join(*l, join_ordering)?,
                self.create_logical_join(*r, join_ordering)?,
                JoinType::Inner,
                Condition::true_cond(),
            )
            .into(),
            (None, None) => {
                if let Some(idx) = join_tree.idx {
                    join_ordering.push(idx);
                    self.inputs[idx].clone()
                } else {
                    return Err(RwError::from(ErrorCode::InternalError(
                        "id of the leaf node not found in the join tree".into(),
                    )));
                }
            }
            (_, _) => {
                return Err(RwError::from(ErrorCode::InternalError(
                    "only leaf node can have None subtree".into(),
                )));
            }
        })
    }
}

// Join tree internal representation
#[derive(Clone, Default, Debug)]
struct JoinTreeNode {
    idx: Option<usize>,
    left: Option<Box<JoinTreeNode>>,
    right: Option<Box<JoinTreeNode>>,
    height: usize,
}

// join graph internal representation
#[derive(Clone, Debug)]
struct GraphNode {
    id: usize,
    join_tree: JoinTreeNode,
    relations: BTreeSet<usize>,
}

impl ToStream for LogicalMultiJoin {
    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }

    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ToBatch for LogicalMultiJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ColPrunable for LogicalMultiJoin {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ExprRewritable for LogicalMultiJoin {
    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ExprVisitable for LogicalMultiJoin {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl PredicatePushdown for LogicalMultiJoin {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::InputRef;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::plan_node::generic::GenericPlanRef;
    use crate::optimizer::property::FunctionalDependency;
    #[tokio::test]
    async fn fd_derivation_multi_join() {
        // t1: [v0, v1], t2: [v2, v3, v4], t3: [v5, v6]
        // FD: v0 --> v1, v2 --> { v3, v4 }, {} --> v5
        // On: v0 = 0 AND v1 = v3 AND v4 = v5
        //
        // Output: [v0, v1, v4, v5]
        // FD: v0 --> v1, {} --> v0, {} --> v5, v4 --> v5, v5 --> v4
        let ctx = OptimizerContext::mock().await;
        let t1 = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "v0"),
                Field::with_name(DataType::Int32, "v1"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx.clone());
            // 0 --> 1
            values
                .base
                .functional_dependency_mut()
                .add_functional_dependency_by_column_indices(&[0], &[1]);
            values
        };
        let t2 = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "v2"),
                Field::with_name(DataType::Int32, "v3"),
                Field::with_name(DataType::Int32, "v4"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx.clone());
            // 0 --> 1, 2
            values
                .base
                .functional_dependency_mut()
                .add_functional_dependency_by_column_indices(&[0], &[1, 2]);
            values
        };
        let t3 = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "v5"),
                Field::with_name(DataType::Int32, "v6"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx);
            // {} --> 0
            values
                .base
                .functional_dependency_mut()
                .add_functional_dependency_by_column_indices(&[], &[0]);
            values
        };
        // On: v0 = 0 AND v1 = v3 AND v4 = v5
        let on: ExprImpl = FunctionCall::new(
            Type::And,
            vec![
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        InputRef::new(0, DataType::Int32).into(),
                        ExprImpl::literal_int(0),
                    ],
                )
                .unwrap()
                .into(),
                FunctionCall::new(
                    Type::And,
                    vec![
                        FunctionCall::new(
                            Type::Equal,
                            vec![
                                InputRef::new(1, DataType::Int32).into(),
                                InputRef::new(3, DataType::Int32).into(),
                            ],
                        )
                        .unwrap()
                        .into(),
                        FunctionCall::new(
                            Type::Equal,
                            vec![
                                InputRef::new(4, DataType::Int32).into(),
                                InputRef::new(5, DataType::Int32).into(),
                            ],
                        )
                        .unwrap()
                        .into(),
                    ],
                )
                .unwrap()
                .into(),
            ],
        )
        .unwrap()
        .into();
        let multi_join = LogicalMultiJoin::new(
            vec![t1.into(), t2.into(), t3.into()],
            Condition::with_expr(on),
            vec![0, 1, 4, 5],
        );
        let expected_fd_set: HashSet<_> = [
            FunctionalDependency::with_indices(4, &[0], &[1]),
            FunctionalDependency::with_indices(4, &[], &[0, 3]),
            FunctionalDependency::with_indices(4, &[2], &[3]),
            FunctionalDependency::with_indices(4, &[3], &[2]),
        ]
        .into_iter()
        .collect();
        let fd_set: HashSet<_> = multi_join
            .functional_dependency()
            .as_dependencies()
            .iter()
            .cloned()
            .collect();
        assert_eq!(expected_fd_set, fd_set);
    }
}
