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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail;
use risingwave_common::types::ScalarImpl;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record, column_names_pretty};
use super::{
    BatchIcebergMetadataScan, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef,
    PlanBase, PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIcebergMetadataScan {
    pub base: PlanBase<Logical>,
    pub core: generic::IcebergMetadataScan,
}

impl LogicalIcebergMetadataScan {
    pub fn new(core: generic::IcebergMetadataScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergMetadataScan }

impl Distill for LogicalIcebergMetadataScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut fields = vec![
            (
                "metadata_type",
                Pretty::debug(&self.core.metadata_type.suffix()),
            ),
            ("columns", column_names_pretty(self.schema())),
        ];
        if let Some(content) = &self.core.filter.content {
            fields.push(("content_filter", Pretty::from(content.clone())));
        }
        if let Some(manifest_path) = &self.core.filter.manifest_path {
            fields.push(("manifest_path_filter", Pretty::from(manifest_path.clone())));
        }
        childless_record("LogicalIcebergMetadataScan", fields)
    }
}

impl ColPrunable for LogicalIcebergMetadataScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mut core = self.core.clone();
        core.output_col_idx = required_cols
            .iter()
            .map(|index| core.output_col_idx[*index])
            .collect();
        Self::new(core).into()
    }
}

impl ExprRewritable<Logical> for LogicalIcebergMetadataScan {}

impl ExprVisitable for LogicalIcebergMetadataScan {}

impl PredicatePushdown for LogicalIcebergMetadataScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let mut core = self.core.clone();
        let mut residual = Vec::new();
        for conjunction in predicate.conjunctions {
            let pushed = extract_string_equality(&conjunction, self.schema())
                .is_some_and(|(column_name, value)| push_filter(&mut core, &column_name, value));
            if !pushed {
                residual.push(conjunction);
            }
        }

        let scan: PlanRef = Self::new(core).into();
        if residual.is_empty() {
            scan
        } else {
            LogicalFilter::create(
                scan,
                Condition {
                    conjunctions: residual,
                },
            )
        }
    }
}

fn extract_string_equality(
    expression: &ExprImpl,
    schema: &risingwave_common::catalog::Schema,
) -> Option<(String, String)> {
    let ExprImpl::FunctionCall(function) = expression else {
        return None;
    };
    if function.func_type() != ExprType::Equal {
        return None;
    }
    let [left, right] = function.inputs() else {
        return None;
    };
    let (input_ref, literal) = match (left, right) {
        (ExprImpl::InputRef(input_ref), ExprImpl::Literal(literal))
        | (ExprImpl::Literal(literal), ExprImpl::InputRef(input_ref)) => (input_ref, literal),
        _ => return None,
    };
    let Some(ScalarImpl::Utf8(value)) = literal.get_data() else {
        return None;
    };
    Some((
        schema.fields.get(input_ref.index)?.name.clone(),
        value.to_string(),
    ))
}

fn push_filter(core: &mut generic::IcebergMetadataScan, column_name: &str, value: String) -> bool {
    let slot = match (core.metadata_type, column_name) {
        (
            risingwave_connector::sink::iceberg::IcebergMetadataTableType::Manifests
            | risingwave_connector::sink::iceberg::IcebergMetadataTableType::Files,
            "content",
        ) => &mut core.filter.content,
        (risingwave_connector::sink::iceberg::IcebergMetadataTableType::Manifests, "path")
        | (risingwave_connector::sink::iceberg::IcebergMetadataTableType::Files, "manifest_path") => {
            &mut core.filter.manifest_path
        }
        _ => return false,
    };

    match slot {
        Some(existing) => existing == &value,
        None => {
            *slot = Some(value);
            true
        }
    }
}

impl ToBatch for LogicalIcebergMetadataScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        Ok(BatchIcebergMetadataScan::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalIcebergMetadataScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        bail!("Iceberg metadata relations are not supported in streaming queries")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail!("Iceberg metadata relations are not supported in streaming queries")
    }
}
