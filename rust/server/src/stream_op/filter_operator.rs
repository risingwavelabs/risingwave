use super::{ExprFn, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::{array2::DataChunk, buffer::Bitmap, error::Result};
use async_trait::async_trait;

/// `FilterOperator` filters data with the `expr`. The `expr` takes a chunk of data,
/// and returns a boolean array on whether each item should be retained. And then,
/// `FilterOperator` will insert, delete or update element into next operator according
/// to the result of the expression.
pub struct FilterOperator {
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Expression of the current filter, note that the filter must always have the same output for the same input.
    expr: Box<dyn ExprFn>,
}

impl FilterOperator {
    pub fn new(output: Box<dyn Output>, expr: Box<dyn ExprFn>) -> Self {
        Self { output, expr }
    }
}

impl StreamOperator for FilterOperator {}

#[async_trait]
impl UnaryStreamOperator for FilterOperator {
    async fn consume(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
            cardinality,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder()
                .columns(arrays)
                .cardinality(cardinality);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        let pred_output = (self.expr)(&data_chunk)?;

        let DataChunk {
            columns: arrays,
            visibility,
            ..
        } = data_chunk;

        let n = ops.len();

        // TODO: Can we update ops and visibility inplace?
        let mut new_ops = Vec::with_capacity(n);
        let mut new_visibility = Vec::with_capacity(n);
        let mut new_cardinality = 0usize;
        let mut last_res = false;

        assert_eq!(pred_output.len(), n);
        assert!(match visibility {
            None => true,
            Some(ref m) => m.len() == n,
        });

        for (i, (op, res)) in ops.into_iter().zip(pred_output.iter()).enumerate() {
            // SAFETY: ops.len() == pred_output.len() == visibility.len()
            if let Some(true) = unsafe { visibility.as_ref().map(|m| m.is_set_unchecked(i)) } {
                match op {
                    Op::Insert | Op::Delete => {
                        new_ops.push(op);
                        if res {
                            new_visibility.push(true);
                            new_cardinality += 1;
                        } else {
                            new_visibility.push(false);
                        }
                    }
                    Op::UpdateDelete => {
                        last_res = res;
                    }
                    Op::UpdateInsert => match (last_res, res) {
                        (true, false) => {
                            new_ops.push(Op::Delete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(true);
                            new_visibility.push(false);
                            new_cardinality += 1;
                        }
                        (false, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::Insert);
                            new_visibility.push(false);
                            new_visibility.push(true);
                            new_cardinality += 1;
                        }
                        (true, true) => {
                            new_ops.push(Op::UpdateDelete);
                            new_ops.push(Op::UpdateInsert);
                            new_visibility.push(true);
                            new_visibility.push(true);
                            new_cardinality += 2;
                        }
                        (false, false) => {
                            new_ops.push(Op::Insert);
                            new_ops.push(Op::Insert);
                            new_visibility.push(false);
                            new_visibility.push(false);
                        }
                    },
                }
            } else {
                new_ops.push(op);
                new_visibility.push(false);
            }
        }

        let new_chunk = StreamChunk {
            columns: arrays,
            visibility: Some(Bitmap::from_vec(new_visibility)?),
            cardinality: new_cardinality,
            ops: new_ops,
        };

        self.output.collect(new_chunk).await
    }
}
