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
use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
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
use either::Either;
use futures_async_stream::try_stream;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::types::{DataType, DatumRef};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{self, BoxedExpression};
use risingwave_expr::table_function::{self, BoxedTableFunction, TableFunctionOutputIter};
use risingwave_pb::expr::project_set_select_item::SelectItem as PbSelectItem;

use crate::datafusion::to_datafusion_error;
use crate::expr::ExprImpl;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectSet {
    input: Arc<LogicalPlan>,
    select_list: Vec<ExprImpl>,
    schema: DFSchemaRef,
}

impl ProjectSet {
    pub fn new(input: Arc<LogicalPlan>, select_list: Vec<ExprImpl>, schema: DFSchemaRef) -> Self {
        Self {
            input,
            select_list,
            schema,
        }
    }
}

impl PartialOrd for ProjectSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        if self.select_list == other.select_list {
            return Some(Ordering::Equal);
        }
        let left = format!("{:?}", self.select_list);
        let right = format!("{:?}", other.select_list);
        Some(left.cmp(&right))
    }
}

impl UserDefinedLogicalNodeCore for ProjectSet {
    fn name(&self) -> &str {
        "ProjectSet"
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
        write!(f, "ProjectSet: select_list={:?}", self.select_list)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<DFExpr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        let [] = exprs.try_into().map_err(|_| {
            exec_datafusion_err!(
                "ProjectSetLogicalPlan: unexpected number of expressions for rewrite"
            )
        })?;
        let [input] = inputs.try_into().map_err(|_| {
            exec_datafusion_err!("ProjectSetLogicalPlan: unexpected number of inputs for rewrite")
        })?;
        Ok(Self {
            input: Arc::new(input),
            select_list: self.select_list.clone(),
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug)]
enum ProjectSetSelectItem {
    Scalar(BoxedExpression),
    Set(BoxedTableFunction),
}
type ProjectSetSelectItems = Vec<Arc<ProjectSetSelectItem>>;

impl ProjectSetSelectItem {
    async fn eval<'a>(
        &'a self,
        input: &'a DataChunk,
    ) -> DFResult<Either<TableFunctionOutputIter<'a>, ArrayRef>> {
        match self {
            Self::Set(tf) => {
                let stream = tf.eval(input).await;
                let iter = TableFunctionOutputIter::new(stream)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Either::Left(iter))
            }
            Self::Scalar(expr) => Ok(Either::Right(
                expr.eval(input).await.map_err(to_datafusion_error)?,
            )),
        }
    }

    fn return_type(&self) -> DataType {
        match self {
            Self::Scalar(expr) => expr.return_type(),
            Self::Set(tf) => tf.return_type(),
        }
    }
}

fn build_select_items(
    select_list: &[ExprImpl],
    chunk_size: usize,
) -> DFResult<ProjectSetSelectItems> {
    select_list
        .iter()
        .map(|expr| {
            let proto = expr.to_project_set_select_item_proto();
            let Some(select_item) = proto.select_item.as_ref() else {
                return exec_err!("ProjectSetSelectItem: missing select item");
            };
            let item = match select_item {
                PbSelectItem::Expr(expr) => ProjectSetSelectItem::Scalar(
                    expr::build_from_prost(expr).map_err(to_datafusion_error)?,
                ),
                PbSelectItem::TableFunction(tf) => ProjectSetSelectItem::Set(
                    table_function::build_from_prost(tf, chunk_size)
                        .map_err(to_datafusion_error)?,
                ),
            };
            Ok(Arc::new(item))
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ProjectSetExec {
    input: Arc<dyn ExecutionPlan>,
    select_items: ProjectSetSelectItems,
    schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl ProjectSetExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        select_items: ProjectSetSelectItems,
        schema: ArrowSchemaRef,
    ) -> Self {
        let plan_properties = Self::compute_properties(&input, schema.clone());
        Self {
            input,
            select_items,
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

// TODO: improve display info with select items
impl DisplayAs for ProjectSetExec {
    fn fmt_as(
        &self,
        format_type: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        if format_type == DisplayFormatType::TreeRender {
            return Ok(());
        }

        write!(f, "ProjectSetExec")?;
        Ok(())
    }
}

impl ExecutionPlan for ProjectSetExec {
    fn name(&self) -> &str {
        "ProjectSetExec"
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
            .map_err(|_| exec_datafusion_err!("ProjectSetExec: unexpected number of children"))?;
        Ok(Arc::new(ProjectSetExec::new(
            child,
            self.select_items.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let chunk_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        let stream = project_set_stream(
            input,
            self.schema.clone(),
            self.select_items.clone(),
            chunk_size,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

// imitate risingwave_batch_executors::executor::project_set::ProjectSetExecutor::do_execute
#[try_stream(ok = RecordBatch, error = datafusion_common::DataFusionError)]
async fn project_set_stream(
    input: SendableRecordBatchStream,
    schema: ArrowSchemaRef,
    select_items: ProjectSetSelectItems,
    chunk_size: usize,
) {
    // First column will be `projected_row_id`, which represents the index in the
    // output table
    let return_types: Vec<DataType> = std::iter::once(DataType::Int64)
        .chain(select_items.iter().map(|item| item.return_type()))
        .collect();
    let mut builder = DataChunkBuilder::new(return_types, chunk_size);
    // a temporary row buffer
    let mut row = vec![None as DatumRef<'_>; builder.num_columns()];

    #[for_await]
    for input_batch in input {
        let input_batch = input_batch?;
        let input_chunk = IcebergArrowConvert
            .chunk_from_record_batch(&input_batch)
            .map_err(to_datafusion_error)?;

        let mut results = Vec::with_capacity(select_items.len());
        for select_item in &select_items {
            results.push(select_item.eval(&input_chunk).await?);
        }

        // for each input row
        for row_idx in 0..input_chunk.capacity() {
            // for each output row
            for projected_row_id in 0i64.. {
                // SAFETY:
                // We use `row` as a buffer and don't read elements from the previous loop.
                // The `transmute` is used for bypassing the borrow checker.
                let row: &mut [DatumRef<'_>] = unsafe { std::mem::transmute(row.as_mut_slice()) };
                row[0] = Some(projected_row_id.into());
                // if any of the set columns has a value
                let mut valid = false;
                // for each column
                for (item, value) in results.iter_mut().zip_eq_fast(&mut row[1..]) {
                    *value = match item {
                        Either::Left(state) => {
                            if let Some((i, value)) = state.peek()
                                && i == row_idx
                            {
                                valid = true;
                                value.map_err(to_datafusion_error)?
                            } else {
                                None
                            }
                        }
                        Either::Right(array) => array.value_at(row_idx),
                    };
                }
                if !valid {
                    // no more output rows for the input row
                    break;
                }
                if let Some(chunk) = builder.append_one_row(&*row) {
                    let record_batch = IcebergArrowConvert
                        .to_record_batch(schema.clone(), &chunk)
                        .map_err(to_datafusion_error)?;
                    yield record_batch;
                }
                // move to the next row
                for item in &mut results {
                    if let Either::Left(state) = item
                        && matches!(state.peek(), Some((i, _)) if i == row_idx)
                    {
                        state.next().await.map_err(to_datafusion_error)?;
                    }
                }
            }
        }

        if let Some(chunk) = builder.consume_all() {
            let record_batch = IcebergArrowConvert
                .to_record_batch(schema.clone(), &chunk)
                .map_err(to_datafusion_error)?;
            yield record_batch;
        }
    }
}

#[derive(Debug)]
pub struct ProjectSetPlanner;

#[async_trait]
impl ExtensionPlanner for ProjectSetPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion::logical_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(project_set) = node.as_any().downcast_ref::<ProjectSet>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return exec_err!("ProjectSetPlanner: unexpected number of physical inputs");
        }
        let chunk_size = session_state.config().batch_size();
        let select_list = build_select_items(&project_set.select_list, chunk_size)?;
        let schema = project_set.schema().inner().clone();
        Ok(Some(Arc::new(ProjectSetExec::new(
            physical_inputs[0].clone(),
            select_list,
            schema,
        ))))
    }
}
