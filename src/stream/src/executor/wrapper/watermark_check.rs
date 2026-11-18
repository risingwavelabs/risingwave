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

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use risingwave_common::array::Op;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::{DefaultOrd, ScalarImpl};
use risingwave_pb::stream_plan::stream_node::PbStreamKind;

use crate::executor::{ExecutorInfo, Message, MessageStream};

/// Streams wrapped by `watermark_check` will check that no row falls below a watermark emitted
/// earlier on the same column. A delete of an upsert stream is not checked, as only its stream key
/// identifies the row it deletes.
///
/// It is a combinator rather than a generator, which would deepen the stack of every executor in
/// debug builds.
pub fn watermark_check(info: Arc<ExecutorInfo>, input: impl MessageStream) -> impl MessageStream {
    let mut watermarks = BTreeMap::<usize, ScalarImpl>::new();
    input.inspect(move |message| match message {
        Ok(Message::Watermark(watermark)) => {
            let emitted = watermarks
                .entry(watermark.col_idx)
                .or_insert_with(|| watermark.val.clone());
            if watermark.val.default_cmp(emitted).is_gt() {
                *emitted = watermark.val.clone();
            }
        }
        Ok(Message::Chunk(chunk)) if !watermarks.is_empty() => {
            for (op, row) in chunk.rows() {
                if info.stream_kind == PbStreamKind::Upsert
                    && matches!(op, Op::Delete | Op::UpdateDelete)
                {
                    continue;
                }
                for (&col_idx, watermark) in &watermarks {
                    if let Some(value) = row.datum_at(col_idx) {
                        debug_assert!(
                            value.default_cmp(&watermark.as_scalar_ref_impl()).is_ge(),
                            "row below a watermark emitted earlier
row: {}
op: {:?}
column: {}
watermark: {:?}
executor: {}",
                            row.display(),
                            op,
                            col_idx,
                            watermark,
                            info.identity
                        );
                    }
                }
            }
        }
        _ => {}
    })
}

#[cfg(test)]
mod tests {
    use futures::pin_mut;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::executor::test_utils::MockSource;

    /// Runs `chunk` after `Watermark(10)` on the only column through the check.
    async fn check_after_watermark(stream_kind: PbStreamKind, chunk: &str) {
        let (mut tx, source) = MockSource::channel();
        let source =
            source.into_executor(Schema::new(vec![Field::unnamed(DataType::Int64)]), vec![]);
        let mut info = source.info().clone();
        info.stream_kind = stream_kind;
        tx.push_int64_watermark(0, 10);
        tx.push_chunk(StreamChunk::from_pretty(chunk));

        let checked = watermark_check(info.into(), source.execute());
        pin_mut!(checked);
        checked.next().await.unwrap().unwrap();
        checked.next().await.unwrap().unwrap();
    }

    #[should_panic(expected = "row below a watermark emitted earlier")]
    #[tokio::test]
    async fn test_row_below_watermark() {
        check_after_watermark(PbStreamKind::Retract, " I\n + 10\n - 9").await;
    }

    #[tokio::test]
    async fn test_rows_not_below_watermark() {
        check_after_watermark(PbStreamKind::Retract, " I\n + 10\n + 11\n + .").await;
        check_after_watermark(PbStreamKind::Upsert, " I\n - 9").await;
    }
}
