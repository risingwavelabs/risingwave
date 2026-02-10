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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, new_null_array};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr as DFExpr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchemaRef, Result as DFResult, exec_datafusion_err, exec_err};
use futures_async_stream::try_stream;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::util::iter_util::ZipEqFast;

#[derive(Debug, Clone, PartialEq, Eq, Hash, educe::Educe)]
#[educe(PartialOrd)]
pub struct Expand {
    input: Arc<LogicalPlan>,
    column_subsets: Vec<Vec<usize>>,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl Expand {
    pub fn new(
        input: Arc<LogicalPlan>,
        column_subsets: Vec<Vec<usize>>,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            input,
            column_subsets,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for Expand {
    fn name(&self) -> &str {
        "Expand"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<DFExpr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expand: column_subsets={:?}", self.column_subsets)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<DFExpr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        let [] = exprs
            .try_into()
            .map_err(|_| exec_datafusion_err!("ExpandLogicalPlan: unexpected expressions"))?;
        let [input] = inputs
            .try_into()
            .map_err(|_| exec_datafusion_err!("ExpandLogicalPlan: unexpected inputs"))?;
        Ok(Self {
            input: Arc::new(input),
            column_subsets: self.column_subsets.clone(),
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    column_subsets: Vec<Vec<usize>>,
    schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl ExpandExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        column_subsets: Vec<Vec<usize>>,
        schema: ArrowSchemaRef,
    ) -> Self {
        let plan_properties = Self::compute_properties(&input, schema.clone());
        Self {
            input,
            column_subsets,
            schema,
            plan_properties,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().to_owned(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(
        &self,
        format_type: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        if format_type == DisplayFormatType::TreeRender {
            write!(f, "column_subsets={:?} ", self.column_subsets)?;
            return Ok(());
        }
        write!(f, "ExpandExec: column_subsets={:?}", self.column_subsets)
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [child] = children
            .try_into()
            .map_err(|_| exec_datafusion_err!("ExpandExec: unexpected number of children"))?;
        Ok(Arc::new(ExpandExec::new(
            child,
            self.column_subsets.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = expand_stream(input, self.schema.clone(), self.column_subsets.clone());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

// imitate risingwave_batch_executors::executor::ExpandExecutor::do_execute
#[try_stream(ok = RecordBatch, error = datafusion_common::DataFusionError)]
async fn expand_stream(
    input: SendableRecordBatchStream,
    schema: ArrowSchemaRef,
    column_subsets: Vec<Vec<usize>>,
) {
    let input_len = (schema.fields().len() - 1) / 2;
    let subset_masks: Vec<Bitmap> = column_subsets
        .iter()
        .map(|subset| Bitmap::from_indices(input_len, subset))
        .collect();

    #[for_await]
    for input_batch in input {
        let input_batch = input_batch?;
        let input_rows = input_batch.num_rows();
        let input_cols = input_batch.columns();
        if input_cols.len() != input_len {
            return exec_err!("ExpandExec: unexpected number of input columns");
        }

        for (flag, subset_mask) in subset_masks.iter().enumerate() {
            let flag = i64::try_from(flag)
                .map_err(|_| exec_datafusion_err!("ExpandExec: flag index overflows i64"))?;
            let flags: ArrayRef =
                Arc::new(Int64Array::from_iter(std::iter::repeat_n(flag, input_rows)));

            let mut output_cols = Vec::with_capacity(input_len * 2 + 1);
            for (input_col, keep) in input_cols.iter().zip_eq_fast(subset_mask.iter()) {
                if keep {
                    output_cols.push(input_col.clone());
                } else {
                    output_cols.push(new_null_array(input_col.data_type(), input_rows));
                }
            }
            output_cols.extend(input_cols.iter().cloned());
            output_cols.push(flags);

            yield RecordBatch::try_new(schema.clone(), output_cols)?;
        }
    }
}

#[derive(Debug)]
pub struct ExpandPlanner;

#[async_trait]
impl ExtensionPlanner for ExpandPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion::logical_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(expand) = node.as_any().downcast_ref::<Expand>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return exec_err!("ExpandPlanner: unexpected number of physical inputs");
        }
        let schema = expand.schema().inner().clone();
        Ok(Some(Arc::new(ExpandExec::new(
            physical_inputs[0].clone(),
            expand.column_subsets.clone(),
            schema,
        ))))
    }
}
