use super::{Message, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::impl_consume_barrier_default;
use crate::{
    array2::{column::Column, DataChunk},
    error::Result,
    expr::BoxedExpression,
};
use async_trait::async_trait;

/// `ProjectionOperator` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data_chunck.  And then, `ProjectionOperator` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectionOperator {
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Expression of the current filter, note that the filter must always have the same output for the same input.
    exprs: Vec<BoxedExpression>,
}

impl ProjectionOperator {
    pub fn new(output: Box<dyn Output>, exprs: Vec<BoxedExpression>) -> Self {
        Self { output, exprs }
    }
}

impl_consume_barrier_default!(ProjectionOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for ProjectionOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns,
            visibility,
            cardinality,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder()
                .columns(columns)
                .cardinality(cardinality);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        // FIXME: unnecessary compact.
        // See https://github.com/singularity-data/risingwave/issues/704
        let data_chunk = data_chunk.compact()?;

        let projected_columns = self
            .exprs
            .iter_mut()
            .map(|expr| {
                expr.eval(&data_chunk)
                    .map(|array| Column::new(array, expr.return_type_ref()))
            })
            .collect::<Result<Vec<Column>>>()?;

        drop(data_chunk);

        let new_chunk = StreamChunk {
            ops,
            columns: projected_columns,
            visibility: None,
            cardinality,
        };

        self.output.collect(Message::Chunk(new_chunk)).await
    }
}
