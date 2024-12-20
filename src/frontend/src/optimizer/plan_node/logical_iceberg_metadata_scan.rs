// Copyright 2024 RisingWave Labs
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

use pretty_xmlish::XmlNode;
use risingwave_connector::source::iceberg::iceberg_metadata_table_schema;
use risingwave_connector::{WithOptionsSecResolved, WithPropertiesExt};
use risingwave_pb::batch_plan::iceberg_metadata_scan_node::IcebergMetadataTableType;

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, BatchIcebergMetadataScan, ColPrunable, ExprRewritable, Logical, LogicalProject,
    PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};
use crate::OptimizerContextRef;

/// `LogicalIcebergMetadataScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergMetadataScan`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIcebergMetadataScan {
    pub base: PlanBase<Logical>,
    pub core: generic::IcebergMetadataScan,
}

impl LogicalIcebergMetadataScan {
    pub fn new(
        iceberg_properties: WithOptionsSecResolved,
        table_type: IcebergMetadataTableType,
        ctx: OptimizerContextRef,
    ) -> Self {
        assert!(iceberg_properties.is_iceberg_connector());
        let schema = iceberg_metadata_table_schema(table_type);

        let core = generic::IcebergMetadataScan {
            schema,
            iceberg_properties,
            table_type,
            ctx,
        };
        let base = PlanBase::new_logical_with_core(&core);

        LogicalIcebergMetadataScan { base, core }
    }
}

impl_plan_tree_node_for_leaf! {LogicalIcebergMetadataScan}
impl Distill for LogicalIcebergMetadataScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![
            ("table_type", self.core.table_type.as_str_name().into()),
            ("columns", column_names_pretty(self.schema())),
        ];
        childless_record("LogicalIcebergMetadataScan", fields)
    }
}

impl ColPrunable for LogicalIcebergMetadataScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // no pruning
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalIcebergMetadataScan {}

impl ExprVisitable for LogicalIcebergMetadataScan {}

impl PredicatePushdown for LogicalIcebergMetadataScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalIcebergMetadataScan {
    fn to_batch(&self) -> Result<PlanRef> {
        let plan: PlanRef = BatchIcebergMetadataScan::new(self.core.clone()).into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergMetadataScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
