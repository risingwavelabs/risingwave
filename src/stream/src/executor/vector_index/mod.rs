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

use anyhow::anyhow;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::catalog::TableId;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::ScalarRef;
use risingwave_common::util::value_encoding::{BasicSerializer, ValueRowSerializer};
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    InitOptions, NewVectorWriterOptions, SealCurrentEpochOptions, StateStoreWriteEpochControl,
    StateStoreWriteVector,
};

use crate::executor::prelude::try_stream;
use crate::executor::{
    BoxedMessageStream, Execute, Executor, Message, StreamExecutorError, StreamExecutorResult,
    expect_first_barrier,
};

pub struct VectorIndexWriteExecutor<S: StateStore> {
    input: Executor,
    vector_writer: S::VectorWriter,
    serializer: BasicSerializer,
}

impl<S: StateStore> Execute for VectorIndexWriteExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        Box::pin(self.execute_inner())
    }
}

impl<S: StateStore> VectorIndexWriteExecutor<S> {
    pub async fn new(input: Executor, store: S, table_id: TableId) -> StreamExecutorResult<Self> {
        let vector_writer = store
            .new_vector_writer(NewVectorWriterOptions { table_id })
            .await;
        Ok(Self {
            input,
            vector_writer,
            serializer: BasicSerializer,
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(mut self) {
        let info_column_indices = (1..self.input.schema().len()).collect_vec();
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.vector_writer
            .init(InitOptions { epoch: first_epoch })
            .await?;

        while let Some(msg) = input.try_next().await? {
            match msg {
                Message::Barrier(barrier) => {
                    self.vector_writer.flush().await?;
                    self.vector_writer.seal_current_epoch(
                        barrier.epoch.curr,
                        SealCurrentEpochOptions {
                            table_watermarks: None,
                            switch_op_consistency_level: None,
                        },
                    );
                    yield Message::Barrier(barrier);
                }
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        if op != Op::Insert {
                            return Err(anyhow!(
                                "should be append-only for vector index writer but receive op {:?}",
                                op
                            )
                            .into());
                        }
                        let vector_datum = row.datum_at(0);
                        let Some(vector_datum) = vector_datum else {
                            warn!(
                                ?row,
                                "vector index writer received a row with null vector datum, skipping"
                            );
                            continue;
                        };
                        let vector = vector_datum.into_vector();
                        let vector = vector.to_owned_scalar();
                        let info = self
                            .serializer
                            .serialize(row.project(&info_column_indices))
                            .into();
                        self.vector_writer.insert(vector, info)?;
                    }
                    self.vector_writer.try_flush().await?;
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}
