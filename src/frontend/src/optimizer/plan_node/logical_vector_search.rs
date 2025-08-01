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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::array::VECTOR_DISTANCE_TYPE;
use risingwave_common::bail;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::common::PbDistanceType;
use risingwave_pb::plan_common::JoinType;

use crate::OptimizerContextRef;
use crate::error::ErrorCode;
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
    TableFunction, TableFunctionType, collect_input_refs,
};
use crate::optimizer::plan_node::batch_vector_search::BatchVectorSearchCore;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{
    GenericPlanNode, GenericPlanRef, TopNLimit, ensure_sorted_required_cols,
};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{LogicalPlanRef as PlanRef, *};
use crate::optimizer::property::{FunctionalDependencySet, Order};
use crate::optimizer::rule::IndexSelectionRule;
use crate::utils::{ColIndexMappingRewriteExt, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorSearchCore {
    top_n: u64,
    distance_type: PbDistanceType,
    left: ExprImpl,
    right: ExprImpl,
    /// The indices of input that will be included in the output.
    /// The index of distance column is `output_indices.len()`
    output_indices: Vec<usize>,
    input: PlanRef,
}

impl VectorSearchCore {
    pub(crate) fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            top_n: self.top_n,
            distance_type: self.distance_type,
            left: self.left.clone(),
            right: self.right.clone(),
            output_indices: self.output_indices.clone(),
            input,
        }
    }

    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.left = r.rewrite_expr(self.left.clone());
        self.right = r.rewrite_expr(self.right.clone());
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        v.visit_expr(&self.left);
        v.visit_expr(&self.right);
    }

    pub(crate) fn i2o_mapping(&self) -> ColIndexMapping {
        let mut mapping = vec![None; self.input.schema().len()];
        for (output_idx, input_idx) in self.output_indices.iter().enumerate() {
            mapping[*input_idx] = Some(output_idx);
        }
        ColIndexMapping::new(mapping, self.output_indices.len() + 1)
    }
}

impl GenericPlanNode for VectorSearchCore {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.i2o_mapping()
            .rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }

    fn schema(&self) -> Schema {
        let fields = self
            .output_indices
            .iter()
            .map(|idx| self.input.schema()[*idx].clone())
            .chain([Field::new("vector_distance", DataType::Float64)])
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.input.stream_key().and_then(|v| {
            let i2o_mapping = self.i2o_mapping();
            v.iter().map(|idx| i2o_mapping.try_map(*idx)).collect()
        })
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalVectorSearch {
    pub base: PlanBase<Logical>,
    core: VectorSearchCore,
}

impl LogicalVectorSearch {
    pub(crate) fn new(
        top_n: u64,
        distance_type: PbDistanceType,
        left: ExprImpl,
        right: ExprImpl,
        output_indices: Vec<usize>,
        input: PlanRef,
    ) -> Self {
        let core = VectorSearchCore {
            top_n,
            distance_type,
            left,
            right,
            output_indices,
            input,
        };
        Self::with_core(core)
    }

    fn with_core(core: VectorSearchCore) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub(crate) fn i2o_mapping(&self) -> ColIndexMapping {
        self.core.i2o_mapping()
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalVectorSearch }

impl PlanTreeNodeUnary<Logical> for LogicalVectorSearch {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let core = self.core.clone_with_input(input);
        Self::with_core(core)
    }
}

impl Distill for LogicalVectorSearch {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 4 } else { 6 });
        vec.push(("distance_type", Pretty::debug(&self.core.distance_type)));
        vec.push(("top_n", Pretty::debug(&self.core.top_n)));
        vec.push(("left", Pretty::debug(&self.core.left)));
        vec.push(("right", Pretty::debug(&self.core.right)));

        if verbose {
            vec.push((
                "output_columns",
                Pretty::Array(
                    self.core
                        .output_indices
                        .iter()
                        .map(|input_idx| {
                            Pretty::debug(&self.core.input.schema().fields()[*input_idx])
                        })
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalVectorSearch", vec)
    }
}

impl ColPrunable for LogicalVectorSearch {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let (project_exprs, required_cols) =
            ensure_sorted_required_cols(required_cols, self.base.schema());
        assert!(required_cols.is_sorted());
        let input_schema = self.core.input.schema();
        let mut required_input_idx_bitset =
            collect_input_refs(input_schema.len(), [&self.core.left, &self.core.right]);
        let mut non_distance_required_input_idx = Vec::new();
        let require_distance_col = required_cols
            .last()
            .map(|last_col_idx| *last_col_idx == self.core.output_indices.len())
            .unwrap_or(false);
        let non_distance_iter_end_idx = if require_distance_col {
            required_cols.len() - 1
        } else {
            required_cols.len()
        };
        for &required_col_idx in &required_cols[0..non_distance_iter_end_idx] {
            let required_input_idx = self.core.output_indices[required_col_idx];
            non_distance_required_input_idx.push(required_input_idx);
            required_input_idx_bitset.set(required_col_idx, true);
        }
        let input_required_idx = required_input_idx_bitset.ones().collect_vec();

        let new_input = self.input().prune_col(&input_required_idx, ctx);
        // mapping from idx of original input to new input
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_idx,
            self.input().schema().len(),
        );

        let vector_search = {
            let mut new_core = self.core.clone_with_input(new_input);
            new_core.left = mapping.rewrite_expr(new_core.left);
            new_core.right = mapping.rewrite_expr(new_core.right);
            new_core.output_indices = non_distance_required_input_idx
                .iter()
                .map(|input_idx| mapping.map(*input_idx))
                .collect();
            Self::with_core(new_core)
        };
        LogicalProject::create(vector_search.into(), project_exprs)
    }
}

impl ExprRewritable<Logical> for LogicalVectorSearch {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::with_core(core).into()
    }
}

impl ExprVisitable for LogicalVectorSearch {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalVectorSearch {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToStream for LogicalVectorSearch {
    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> crate::error::Result<(PlanRef, ColIndexMapping)> {
        bail!("LogicalVectorSearch can only for batch plan, not stream plan");
    }

    fn to_stream(&self, _ctx: &mut ToStreamContext) -> crate::error::Result<StreamPlanRef> {
        bail!("LogicalVectorSearch can only for batch plan, not stream plan");
    }
}

impl LogicalVectorSearch {
    fn to_top_n(&self) -> LogicalTopN {
        let (neg, expr_type) = match self.core.distance_type {
            PbDistanceType::Unspecified => {
                unreachable!()
            }
            PbDistanceType::L1 => (false, ExprType::L1Distance),
            PbDistanceType::L2Sqr => (false, ExprType::L2Distance),
            PbDistanceType::Cosine => (false, ExprType::CosineDistance),
            PbDistanceType::InnerProduct => (true, ExprType::InnerProduct),
        };
        let mut expr = ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
            expr_type,
            vec![self.core.left.clone(), self.core.right.clone()],
            VECTOR_DISTANCE_TYPE,
        )));
        if neg {
            expr = ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
                ExprType::Neg,
                vec![expr],
                VECTOR_DISTANCE_TYPE,
            )));
        }
        let exprs = generic::Project::out_col_idx_exprs(
            &self.core.input,
            self.core.output_indices.iter().copied(),
        )
        .chain([expr])
        .collect();

        let input = LogicalProject::new(self.input(), exprs).into();
        let top_n = generic::TopN::without_group(
            input,
            TopNLimit::Simple(self.core.top_n),
            0,
            Order::new(vec![ColumnOrder::new(
                self.core.output_indices.len(),
                OrderType::ascending(),
            )]),
        );
        top_n.into()
    }

    fn as_vector_table_scan(&self) -> Option<(&LogicalScan, ExprImpl, usize)> {
        let scan = self.core.input.as_logical_scan()?;
        if !scan.predicate().always_true() {
            return None;
        }
        let (vector_input, vec) = match (&self.core.left, &self.core.right) {
            (ExprImpl::InputRef(_), ExprImpl::InputRef(_)) => return None,
            (ExprImpl::InputRef(input), other) | (other, ExprImpl::InputRef(input))
                if other.only_literal_and_func() =>
            {
                (input, other.clone())
            }
            _ => return None,
        };
        Some((scan, vec, vector_input.index))
    }
}

impl ToBatch for LogicalVectorSearch {
    fn to_batch(&self) -> crate::error::Result<BatchPlanRef> {
        if let Some((scan, vector_expr, vector_input_idx)) = self.as_vector_table_scan()
            && !scan.vector_indexes().is_empty()
        {
            for index in scan.vector_indexes() {
                if index.vector_column_idx != scan.output_col_idx()[vector_input_idx] {
                    continue;
                }
                if index
                    .index_table
                    .vector_index_info
                    .as_ref()
                    .expect("vector index")
                    .distance_type()
                    != self.core.distance_type
                {
                    continue;
                }

                let primary_table_cols_idx = self
                    .core
                    .output_indices
                    .iter()
                    .map(|input_idx| scan.output_col_idx()[*input_idx])
                    .collect_vec();
                let mut covered_table_cols_idx = Vec::new();
                let mut non_covered_table_cols_idx = Vec::new();
                for table_col_idx in &primary_table_cols_idx {
                    if index
                        .primary_to_included_info_column_mapping
                        .contains_key(table_col_idx)
                    {
                        covered_table_cols_idx.push(*table_col_idx);
                    } else {
                        non_covered_table_cols_idx.push(*table_col_idx);
                    }
                }
                let vector_data_type = vector_expr.return_type();
                let literal_vector_input = BatchValues::new(LogicalValues::new(
                    vec![vec![vector_expr]],
                    Schema::from_iter([Field::unnamed(vector_data_type)]),
                    self.core.ctx(),
                ))
                .into();
                let core = BatchVectorSearchCore {
                    input: literal_vector_input,
                    top_n: self.core.top_n,
                    distance_type: self.core.distance_type,
                    index_name: index.index_table.name.clone(),
                    index_table_id: index.index_table.id,
                    info_column_desc: index.index_table.columns
                        [1..=index.included_info_columns.len()]
                        .iter()
                        .map(|col| col.column_desc.clone())
                        .collect(),
                    vector_column_idx: 0,
                    ctx: self.core.ctx(),
                };
                let vector_search: BatchPlanRef = {
                    let vector_search: BatchPlanRef = BatchVectorSearch::with_core(core).into();
                    let unnested_array: BatchPlanRef = BatchProjectSet::new(generic::ProjectSet {
                        select_list: vec![ExprImpl::TableFunction(
                            TableFunction::new(
                                TableFunctionType::Unnest,
                                vec![ExprImpl::InputRef(
                                    InputRef::new(1, vector_search.schema()[1].data_type()).into(),
                                )],
                            )?
                            .into(),
                        )],
                        input: vector_search,
                    })
                    .into();
                    let DataType::Struct(struct_type) = &unnested_array.schema()[1].data_type
                    else {
                        panic!("{:?}", unnested_array.schema()[1].data_type);
                    };
                    let unnest_struct = BatchProject::new(generic::Project::new(
                        struct_type
                            .types()
                            .enumerate()
                            .map(|(idx, data_type)| {
                                ExprImpl::FunctionCall(
                                    FunctionCall::new_unchecked(
                                        ExprType::Field,
                                        vec![
                                            ExprImpl::InputRef(
                                                InputRef::new(
                                                    1,
                                                    DataType::Struct(struct_type.clone()),
                                                )
                                                .into(),
                                            ),
                                            ExprImpl::Literal(
                                                Literal::new(
                                                    Some(ScalarImpl::Int32(idx as _)),
                                                    DataType::Int32,
                                                )
                                                .into(),
                                            ),
                                        ],
                                        data_type.clone(),
                                    )
                                    .into(),
                                )
                            })
                            .collect(),
                        unnested_array,
                    ));
                    unnest_struct.into()
                };
                let covered_output_col_idx = covered_table_cols_idx.iter().map(|table_col_idx| {
                    index.primary_to_included_info_column_mapping[table_col_idx]
                });
                return Ok(if non_covered_table_cols_idx.is_empty() {
                    BatchProject::new(generic::Project::with_out_col_idx(
                        vector_search,
                        covered_output_col_idx.chain([index.included_info_columns.len()]),
                    ))
                    .into()
                } else {
                    let mut primary_table_cols_idx = Vec::with_capacity(
                        non_covered_table_cols_idx.len() + scan.table().pk().len(),
                    );
                    primary_table_cols_idx.extend(
                        non_covered_table_cols_idx
                            .iter()
                            .cloned()
                            .chain(scan.table().pk().iter().map(|order| order.column_index)),
                    );
                    let table_scan = generic::TableScan::new(
                        primary_table_cols_idx,
                        scan.table().clone(),
                        vec![],
                        vec![],
                        self.core.input.ctx(),
                        Condition::true_cond(),
                        None,
                    );
                    let logical_scan = LogicalScan::from(table_scan);
                    let batch_scan = logical_scan.to_batch()?;
                    let vector_search_schema = vector_search.schema();
                    let on_condition = Condition {
                        conjunctions: index
                            .primary_key_idx_in_info_columns
                            .iter()
                            .zip_eq_debug(0..scan.table().pk().len())
                            .map(|(pk_idx_in_info_columns, pk_idx)| {
                                let batch_scan_pk_idx = vector_search_schema.len()
                                    + non_covered_table_cols_idx.len()
                                    + pk_idx;
                                IndexSelectionRule::create_null_safe_equal_expr(
                                    *pk_idx_in_info_columns,
                                    vector_search_schema[*pk_idx_in_info_columns].data_type(),
                                    batch_scan_pk_idx,
                                    batch_scan.schema()[non_covered_table_cols_idx.len() + pk_idx]
                                        .data_type(),
                                )
                            })
                            .collect(),
                    };
                    let eq_predicate = EqJoinPredicate::create(
                        vector_search_schema.len(),
                        batch_scan.schema().len(),
                        on_condition.clone(),
                    );
                    let join = generic::Join::new(
                        vector_search.clone(),
                        batch_scan,
                        on_condition,
                        JoinType::Inner,
                        covered_output_col_idx
                            .chain((0..non_covered_table_cols_idx.len()).map(
                                |primary_table_output_idx| {
                                    primary_table_output_idx + vector_search_schema.len()
                                },
                            ))
                            .chain([index.included_info_columns.len()])
                            .collect(),
                    );
                    let lookup_join = LogicalJoin::gen_batch_lookup_join(
                        &logical_scan,
                        eq_predicate,
                        join,
                        false,
                    )?
                    .ok_or_else(|| {
                        ErrorCode::InternalError(
                            "failed to convert to batch lookup join".to_owned(),
                        )
                    })?;
                    lookup_join.into()
                });
            }
        }
        self.to_top_n().to_batch()
    }
}
