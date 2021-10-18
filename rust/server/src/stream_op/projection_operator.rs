use super::{Executor, Message, SimpleExecutor, StreamChunk};
use crate::impl_consume_barrier_default;
use crate::{
    array::{column::Column, DataChunk},
    error::Result,
    expr::BoxedExpression,
};
use async_trait::async_trait;

/// `ProjectExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunck. And then, `ProjectExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct ProjectExecutor {
    /// The input of the current operator
    input: Box<dyn Executor>,
    /// Expressions of the current projection.
    exprs: Vec<BoxedExpression>,
}

impl ProjectExecutor {
    pub fn new(input: Box<dyn Executor>, exprs: Vec<BoxedExpression>) -> Self {
        Self { input, exprs }
    }
}

impl_consume_barrier_default!(ProjectExecutor, Executor);

impl SimpleExecutor for ProjectExecutor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let chunk = chunk.compact()?;

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
