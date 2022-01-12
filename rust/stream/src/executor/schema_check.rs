use std::fmt::Debug;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{Executor, Message, PkIndicesRef};

/// [`SchemaCheckExecutor`] checks the passing stream chunk against the expected schema.
///
/// Note that currently this only checks the physical type (variant name).
pub struct SchemaCheckExecutor {
    /// The input of the current executor
    input: Box<dyn Executor>,
}

impl Debug for SchemaCheckExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaCheckExecutor")
            .field("input", &self.input)
            .finish()
    }
}

impl SchemaCheckExecutor {
    pub fn new(input: Box<dyn Executor>) -> Self {
        Self { input }
    }
}

#[async_trait]
impl Executor for SchemaCheckExecutor {
    async fn next(&mut self) -> Result<Message> {
        let message = self.input.next().await?;

        if let Message::Chunk(chunk) = &message {
            for (i, (column, field)) in chunk
                .columns()
                .iter()
                .zip_eq(self.schema().fields())
                .enumerate()
            {
                let array = column.array_ref();
                let builder = field.data_type.create_array_builder(0)?; // TODO: check `data_type` directly

                macro_rules! check_schema {
          ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
            use risingwave_common::array::ArrayBuilderImpl;
            use risingwave_common::array::ArrayImpl;

            match (array, &builder) {
              $( (ArrayImpl::$variant_name(_), ArrayBuilderImpl::$variant_name(_)) => {} ),*
              _ => panic!("schema check failed on {}: column {} should be {}, while stream chunk gives {}",
                          self.input.identity(), i, builder.get_ident(), array.get_ident()),
            }
          };
        }

                for_all_variants! { check_schema };
            }
        }

        Ok(message)
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.input.pk_indices()
    }

    fn identity(&self) -> &'static str {
        "SchemaCheckExecutor"
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::{F64Array, I64Array, Op, StreamChunk};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{Float64Type, Int64Type};

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn test_schema_ok() {
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [100, 10, 4] },
                column_nonnull! { F64Array, [200.0, 14.0, 300.0] },
            ],
            None,
        );
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Float64Type::create(false),
                },
            ],
        };

        let mut source = MockSource::new(schema, vec![1]);
        source.push_chunks([chunk].into_iter());
        source.push_barrier(1, false);

        let mut checked = SchemaCheckExecutor::new(Box::new(source));
        assert_matches!(checked.next().await.unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap(), Message::Barrier(_));
    }

    #[should_panic]
    #[tokio::test]
    async fn test_schema_bad() {
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [100, 10, 4] },
                column_nonnull! { I64Array, [200, 14, 300] },
            ],
            None,
        );
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Float64Type::create(false),
                },
            ],
        };

        let mut source = MockSource::new(schema, vec![1]);
        source.push_chunks([chunk].into_iter());
        source.push_barrier(1, false);

        let mut checked = SchemaCheckExecutor::new(Box::new(source));
        checked.next().await.unwrap();
    }
}
