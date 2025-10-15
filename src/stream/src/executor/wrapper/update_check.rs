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

use std::sync::Arc;

use futures_async_stream::try_stream;
use risingwave_common::array::stream_record::Record;
use risingwave_common::row::RowExt;

use crate::executor::error::StreamExecutorError;
use crate::executor::{ExecutorInfo, Message, MessageStream};

/// Streams wrapped by `update_check` will check whether the two rows of updates are next to each
/// other.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn update_check(info: Arc<ExecutorInfo>, input: impl MessageStream) {
    #[for_await]
    for message in input {
        let message = message?;

        if let Message::Chunk(chunk) = &message {
            for record in chunk.records() {
                // `chunk.records()` will check U-/U+ pairing
                if let Record::Update { old_row, new_row } = record {
                    let old_pk = old_row.project(&info.pk_indices);
                    let new_pk = new_row.project(&info.pk_indices);
                    debug_assert_eq!(
                        old_pk,
                        new_pk,
                        "U- and U+ should have same stream key
U- row: {}
U- key: {}
U+ row: {}
U+ key: {}
stream key indices: {:?}
executor: {}",
                        old_row.display(),
                        old_pk.display(),
                        new_row.display(),
                        new_pk.display(),
                        info.pk_indices,
                        info.identity
                    )
                }
            }
        }

        yield message;
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, pin_mut};
    use risingwave_common::array::StreamChunk;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[should_panic]
    #[tokio::test]
    async fn test_not_next_to_each_other() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::from_pretty(
            "     I
            U-  114
            U-  514
            U+ 1919
            U+  810",
        ));

        let checked = update_check(source.info().clone().into(), source.execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_first_one_update_insert() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::from_pretty(
            "     I
            U+  114",
        ));

        let checked = update_check(source.info().clone().into(), source.execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_last_one_update_delete() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::from_pretty(
            "        I
            U-     114
            U+     514
            U- 1919810",
        ));

        let checked = update_check(source.info().clone().into(), source.execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[tokio::test]
    async fn test_empty_chunk() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::default());

        let checked = update_check(source.info().clone().into(), source.execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap();
    }
}
