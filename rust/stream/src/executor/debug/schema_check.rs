// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::fmt::Debug;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_common::for_all_variants;
use tracing::event;

use crate::executor::{Executor, Message};

/// [`SchemaCheckExecutor`] checks the passing stream chunk against the expected schema.
///
/// Note that currently this only checks the physical type (variant name).
#[derive(Debug)]
pub struct SchemaCheckExecutor {
    /// The input of the current executor.
    input: Box<dyn Executor>,
}

impl SchemaCheckExecutor {
    pub fn new(input: Box<dyn Executor>) -> Self {
        Self { input }
    }
}

#[async_trait]
impl super::DebugExecutor for SchemaCheckExecutor {
    async fn next(&mut self) -> Result<Message> {
        let message = self.input.next().await?;

        if let Message::Chunk(chunk) = &message {
            event!(
                tracing::Level::TRACE,
                "input schema = \n{:#?}\nexpected schema = \n{:#?}",
                chunk
                    .columns()
                    .iter()
                    .map(|col| col.array_ref().get_ident())
                    .collect_vec(),
                self.schema().fields()
            );

            for (i, pair) in chunk
                .columns()
                .iter()
                .zip_longest(self.schema().fields())
                .enumerate()
            {
                let array = pair.as_ref().left().map(|c| c.array_ref());
                let builder = pair
                    .as_ref()
                    .right()
                    .map(|f| f.data_type.create_array_builder(0).unwrap()); // TODO: check `data_type` directly

                macro_rules! check_schema {
                    ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
                        use risingwave_common::array::ArrayBuilderImpl;
                        use risingwave_common::array::ArrayImpl;

                        match (array, &builder) {
                            $( (Some(ArrayImpl::$variant_name(_)), Some(ArrayBuilderImpl::$variant_name(_))) => {} ),*
                            _ => panic!("schema check failed on {}: column {} should be {:?}, while stream chunk gives {:?}",
                                                    self.input.logical_operator_info(), i, builder.map(|b| b.get_ident()), array.map(|a| a.get_ident())),
                        }
                    };
                }

                for_all_variants! { check_schema };
            }
        }

        Ok(message)
    }

    fn input(&self) -> &dyn Executor {
        self.input.as_ref()
    }

    fn input_mut(&mut self) -> &mut dyn Executor {
        self.input.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::{F64Array, I64Array, Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;

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
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Float64),
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
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Float64),
            ],
        };

        let mut source = MockSource::new(schema, vec![1]);
        source.push_chunks([chunk].into_iter());
        source.push_barrier(1, false);

        let mut checked = SchemaCheckExecutor::new(Box::new(source));
        checked.next().await.unwrap();
    }
}
