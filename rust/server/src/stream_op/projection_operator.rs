use super::{Message, SimpleStreamOperator, StreamChunk, StreamOperator};
use crate::impl_consume_barrier_default;
use crate::{
    array2::{column::Column, DataChunk},
    error::Result,
    expr::BoxedExpression,
};
use async_trait::async_trait;

/// `ProjectionOperator` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunck. And then, `ProjectionOperator` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectionOperator {
    /// The input of the current operator
    input: Box<dyn StreamOperator>,
    /// Expressions of the current projection.
    exprs: Vec<BoxedExpression>,
}

impl ProjectionOperator {
    pub fn new(input: Box<dyn StreamOperator>, exprs: Vec<BoxedExpression>) -> Self {
        Self { input, exprs }
    }
}

impl_consume_barrier_default!(ProjectionOperator, StreamOperator);

impl SimpleStreamOperator for ProjectionOperator {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(columns);
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
        };

        Ok(Message::Chunk(new_chunk))
    }
}
