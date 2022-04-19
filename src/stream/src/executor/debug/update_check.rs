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

use std::fmt::Debug;
use std::iter::once;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::error::Result;

use crate::executor::{Executor, Message};

/// [`UpdateCheckExecutor`] checks whether the two rows of updates are next to each other.
#[derive(Debug)]
pub struct UpdateCheckExecutor {
    /// The input of the current executor.
    input: Box<dyn Executor>,
}

impl UpdateCheckExecutor {
    pub fn new(input: Box<dyn Executor>) -> Self {
        Self { input }
    }
}

#[async_trait]
impl super::DebugExecutor for UpdateCheckExecutor {
    async fn next(&mut self) -> Result<Message> {
        let message = self.input.next().await?;

        if let Message::Chunk(chunk) = &message {
            for ((op1, row1), (op2, row2)) in once(None)
                .chain(chunk.rows().map(Some))
                .chain(once(None))
                .map(|r| (r.unzip()))
                .tuple_windows()
            {
                if (op1 == None && op2 == Some(Op::UpdateInsert)) // the first row is U+
                    || (op1 == Some(Op::UpdateDelete) && op2 != Some(Op::UpdateInsert))
                {
                    panic!(
                        "update check failed on `{}`: expect U+ after  U-:\n first row: {:?}\nsecond row: {:?}",
                        self.input.logical_operator_info(),
                        row1,
                        row2,
                    )
                }
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
    use std::iter::once;

    use risingwave_common::array::{I64Array, StreamChunk};
    use risingwave_common::column_nonnull;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[should_panic]
    #[tokio::test]
    async fn test_not_next_to_each_other() {
        let chunk = StreamChunk::new(
            vec![
                Op::UpdateDelete,
                Op::UpdateDelete,
                Op::UpdateInsert,
                Op::UpdateInsert,
            ],
            vec![column_nonnull! { I64Array, [114, 514, 1919, 810] }],
            None,
        );

        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks(once(chunk));

        let mut checked = UpdateCheckExecutor::new(Box::new(source));
        checked.next().await.unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_first_one_update_insert() {
        let chunk = StreamChunk::new(
            vec![Op::UpdateInsert],
            vec![column_nonnull! { I64Array, [114] }],
            None,
        );

        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks(once(chunk));

        let mut checked = UpdateCheckExecutor::new(Box::new(source));
        checked.next().await.unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_last_one_update_delete() {
        let chunk = StreamChunk::new(
            vec![Op::UpdateDelete, Op::UpdateInsert, Op::UpdateDelete],
            vec![column_nonnull! { I64Array, [114, 514, 1919810] }],
            None,
        );

        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks(once(chunk));

        let mut checked = UpdateCheckExecutor::new(Box::new(source));
        checked.next().await.unwrap(); // should panic
    }

    #[tokio::test]
    async fn test_empty_chunk() {
        let chunk = StreamChunk::default();

        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks(once(chunk));

        let mut checked = UpdateCheckExecutor::new(Box::new(source));
        checked.next().await.unwrap();
    }
}
