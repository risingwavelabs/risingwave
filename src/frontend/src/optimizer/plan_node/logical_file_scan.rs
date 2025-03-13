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

use pretty_xmlish::XmlNode;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    BatchFileScan, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::OptimizerContextRef;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalFileScan {
    pub base: PlanBase<Logical>,
    pub core: generic::FileScanBackend,
}

impl LogicalFileScan {
    pub fn new_s3_logical_file_scan(
        ctx: OptimizerContextRef,
        schema: Schema,
        file_format: String,
        storage_type: String,
        s3_region: String,
        s3_access_key: String,
        s3_secret_key: String,
        file_location: Vec<String>,
        s3_endpoint: String,
    ) -> Self {
        assert!("parquet".eq_ignore_ascii_case(&file_format));
        assert!("s3".eq_ignore_ascii_case(&storage_type));
        let storage_type = generic::StorageType::S3;

        let core = generic::FileScanBackend::FileScan(generic::FileScan {
            schema,
            file_format: generic::FileFormat::Parquet,
            storage_type,
            s3_region,
            s3_access_key,
            s3_secret_key,
            file_location,
            ctx,
            s3_endpoint,
        });

        let base = PlanBase::new_logical_with_core(&core);

        LogicalFileScan { base, core }
    }

    pub fn new_gcs_logical_file_scan(
        ctx: OptimizerContextRef,
        schema: Schema,
        file_format: String,
        storage_type: String,
        credential: String,
        file_location: Vec<String>,
    ) -> Self {
        assert!("parquet".eq_ignore_ascii_case(&file_format));
        assert!("gcs".eq_ignore_ascii_case(&storage_type));

        let core = generic::FileScanBackend::GcsFileScan(generic::GcsFileScan {
            schema,
            file_format: generic::FileFormat::Parquet,
            storage_type: generic::StorageType::Gcs,
            credential,
            file_location,
            ctx,
        });

        let base = PlanBase::new_logical_with_core(&core);
        LogicalFileScan { base, core }
    }

    pub fn new_azblob_logical_file_scan(
        ctx: OptimizerContextRef,
        schema: Schema,
        file_format: String,
        storage_type: String,
        account_name: String,
        account_key: String,
        endpoint: String,
        file_location: Vec<String>,
    ) -> Self {
        assert!("parquet".eq_ignore_ascii_case(&file_format));
        assert!("azblob".eq_ignore_ascii_case(&storage_type));

        let core = generic::FileScanBackend::AzblobFileScan(generic::AzblobFileScan {
            schema,
            file_format: generic::FileFormat::Parquet,
            storage_type: generic::StorageType::Azblob,
            account_name,
            account_key,
            endpoint,
            file_location,
            ctx,
        });

        let base = PlanBase::new_logical_with_core(&core);

        LogicalFileScan { base, core }
    }
}

impl_plan_tree_node_for_leaf! {LogicalFileScan}
impl Distill for LogicalFileScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("LogicalFileScan", fields)
    }
}

impl ColPrunable for LogicalFileScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().cloned()).into()
    }
}

impl ExprRewritable for LogicalFileScan {}

impl ExprVisitable for LogicalFileScan {}

impl PredicatePushdown for LogicalFileScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalFileScan {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchFileScan::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalFileScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail!("file_scan function is not supported in streaming mode")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail!("file_scan function is not supported in streaming mode")
    }
}
