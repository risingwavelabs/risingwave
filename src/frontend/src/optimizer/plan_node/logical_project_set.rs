// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_pb::expr::ProjectSetSelectItem as SelectItemProst;

use super::{
    BatchProjectSet, ColPrunable, LogicalFilter, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamProjectSet, ToBatch, ToStream,
};
use crate::binder::BoundTableFunction;
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::risingwave_common::error::Result;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalProjectSet` projects one row multiple times according to `select_list`.
///
/// Different from `Project`, it supports [`TableFunction`](crate::binder::BoundTableFunction)s.
/// See also [`SelectItemProst`] for examples.
///
/// To have a pk, it has a hidden column `projected_row_id` at the beginning. The implementation of
/// `LogicalProjectSet` is highly similar to [`LogicalProject`], except for the additional hidden
/// column.
#[derive(Debug, Clone)]
pub struct LogicalProjectSet {
    pub base: PlanBase,
    select_list: Vec<ProjectSetSelectItem>,
    input: PlanRef,
}

/// See also [`SelectItemProst`]
#[derive(Debug, Clone)]
pub enum ProjectSetSelectItem {
    TableFunction(BoundTableFunction),
    Expr(ExprImpl),
}

impl From<BoundTableFunction> for ProjectSetSelectItem {
    fn from(table_function: BoundTableFunction) -> Self {
        ProjectSetSelectItem::TableFunction(table_function)
    }
}

impl From<ExprImpl> for ProjectSetSelectItem {
    fn from(expr: ExprImpl) -> Self {
        ProjectSetSelectItem::Expr(expr)
    }
}

impl ProjectSetSelectItem {
    pub fn return_type(&self) -> DataType {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => tf.return_type.clone(),
            ProjectSetSelectItem::Expr(expr) => expr.return_type(),
        }
    }

    pub fn to_protobuf(&self) -> SelectItemProst {
        use risingwave_pb::expr::project_set_select_item::SelectItem::*;

        SelectItemProst {
            select_item: Some(match self {
                ProjectSetSelectItem::TableFunction(tf) => TableFunction(tf.to_protobuf()),
                ProjectSetSelectItem::Expr(expr) => Expr(expr.to_expr_proto()),
            }),
        }
    }

    pub fn rewrite(self, rewriter: &mut impl ExprRewriter) -> Self {
        match self {
            ProjectSetSelectItem::TableFunction(tf) => {
                ProjectSetSelectItem::TableFunction(tf.rewrite(rewriter))
            }
            ProjectSetSelectItem::Expr(expr) => {
                ProjectSetSelectItem::Expr(rewriter.rewrite_expr(expr))
            }
        }
    }
}

impl LogicalProjectSet {
    pub fn new(input: PlanRef, select_list: Vec<ProjectSetSelectItem>) -> Self {
        let ctx = input.ctx();
        let schema = Self::derive_schema(&select_list, input.schema());
        let pk_indices = Self::derive_pk(input.schema(), input.pk_indices(), &select_list);
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalProjectSet {
            base,
            select_list,
            input,
        }
    }

    fn derive_schema(select_list: &[ProjectSetSelectItem], input_schema: &Schema) -> Schema {
        let _o2i = Self::o2i_col_mapping_inner(input_schema.len(), select_list);

        let mut fields = vec![Field::with_name(DataType::Int64, "projected_row_id")];
        fields.extend(select_list.iter().enumerate().map(|(_idx, item)| {
            // TODO: pretty schema like LogicalProject
            // Get field info from o2i.
            // let (name, sub_fields, type_name) = match o2i.try_map(id) {
            //     Some(input_idx) => {
            //         let field = input_schema.fields()[input_idx].clone();
            //         (field.name, field.sub_fields, field.type_name)
            //     }
            //     None => (
            //         format!("{:?}", ExprVerboseDisplay { expr, input_schema }),
            //         vec![],
            //         String::new(),
            //     ),
            // };
            Field::unnamed(item.return_type())
        }));

        Schema { fields }
    }

    fn derive_pk(
        input_schema: &Schema,
        input_pk: &[usize],
        select_list: &[ProjectSetSelectItem],
    ) -> Vec<usize> {
        let i2o = Self::i2o_col_mapping_inner(input_schema.len(), select_list);
        let mut pk = input_pk
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        // add `projected_row_id` to pk
        pk.push(0);
        pk
    }

    pub fn select_list(&self) -> &[ProjectSetSelectItem] {
        &self.select_list
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        let _verbose = self.base.ctx.is_explain_verbose();
        // TODO: add verbose display like Project

        let mut builder = f.debug_struct(name);
        builder.field("select_list", &self.select_list);
        builder.finish()
    }
}

impl LogicalProjectSet {
    /// get the Mapping of columnIndex from output column index to input column index
    fn o2i_col_mapping_inner(
        input_len: usize,
        select_list: &[ProjectSetSelectItem],
    ) -> ColIndexMapping {
        let mut map = vec![None; 1 + select_list.len()];
        for (i, item) in select_list.iter().enumerate() {
            map[1 + i] = match item {
                ProjectSetSelectItem::Expr(ExprImpl::InputRef(input)) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    fn i2o_col_mapping_inner(
        input_len: usize,
        select_list: &[ProjectSetSelectItem],
    ) -> ColIndexMapping {
        Self::o2i_col_mapping_inner(input_len, select_list).inverse()
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        Self::o2i_col_mapping_inner(self.input.schema().len(), self.select_list())
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        Self::i2o_col_mapping_inner(self.input.schema().len(), self.select_list())
    }
}

impl PlanTreeNodeUnary for LogicalProjectSet {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.select_list.clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let select_list = self
            .select_list
            .clone()
            .into_iter()
            .map(|item| item.rewrite(&mut input_col_change))
            .collect();
        let project_set = Self::new(input, select_list);
        // change the input columns index will not change the output column index
        let out_col_change = ColIndexMapping::identity(self.schema().len());
        (project_set, out_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalProjectSet}

impl fmt::Display for LogicalProjectSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalProjectSet")
    }
}

impl ColPrunable for LogicalProjectSet {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        // TODO: column pruning for ProjectSet
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl PredicatePushdown for LogicalProjectSet {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        // TODO: predicate pushdown for ProjectSet
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalProjectSet {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input.to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchProjectSet::new(new_logical).into())
    }
}

impl ToStream for LogicalProjectSet {
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (project_set, out_col_change) =
            self.rewrite_with_input(input.clone(), input_col_change);

        // Add missing columns of input_pk into the select list.
        let input_pk = input.pk_indices();
        let i2o = Self::i2o_col_mapping_inner(input.schema().len(), project_set.select_list());
        let col_need_to_add = input_pk.iter().cloned().filter(|i| i2o.try_map(*i) == None);
        let input_schema = input.schema();
        let select_list = project_set
            .select_list()
            .iter()
            .cloned()
            .chain(col_need_to_add.map(|idx| {
                let input_ref: ExprImpl =
                    InputRef::new(idx, input_schema.fields[idx].data_type.clone()).into();
                input_ref.into()
            }))
            .collect();
        let project_set = Self::new(input, select_list);
        // The added columns is at the end, so it will not change existing column indices.
        // But the target size of `out_col_change` should be the same as the length of the new
        // schema.
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::with_target_size(map, project_set.schema().len());
        Ok((project_set.into(), out_col_change))
    }

    // TODO: implement to_stream_with_dist_required like LogicalProject

    fn to_stream(&self) -> Result<PlanRef> {
        let new_input = self.input().to_stream()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(StreamProjectSet::new(new_logical).into())
    }
}
