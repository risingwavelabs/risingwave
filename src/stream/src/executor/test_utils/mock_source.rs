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

use super::*;
pub struct MockSource {
    rx: mpsc::UnboundedReceiver<Message>,

    /// Whether to send a `Stop` barrier on stream finish.
    stop_on_finish: bool,
}

/// A wrapper around `Sender<Message>`.
pub struct MessageSender(mpsc::UnboundedSender<Message>);

impl MessageSender {
    #[allow(dead_code)]
    pub fn push_chunk(&mut self, chunk: StreamChunk) {
        self.0.send(Message::Chunk(chunk)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_barrier(&mut self, epoch: u64, stop: bool) {
        let mut barrier = Barrier::new_test_barrier(epoch);
        if stop {
            barrier = barrier.with_stop();
        }
        self.0.send(Message::Barrier(barrier)).unwrap();
    }

    pub fn send_barrier(&self, barrier: Barrier) {
        self.0.send(Message::Barrier(barrier)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_barrier_with_prev_epoch_for_test(
        &mut self,
        cur_epoch: u64,
        prev_epoch: u64,
        stop: bool,
    ) {
        let mut barrier = Barrier::with_prev_epoch_for_test(cur_epoch, prev_epoch);
        if stop {
            barrier = barrier.with_stop();
        }
        self.0.send(Message::Barrier(barrier)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_watermark(&mut self, col_idx: usize, data_type: DataType, val: ScalarImpl) {
        self.0
            .send(Message::Watermark(Watermark {
                col_idx,
                data_type,
                val,
            }))
            .unwrap();
    }

    #[allow(dead_code)]
    pub fn push_int64_watermark(&mut self, col_idx: usize, val: i64) {
        self.push_watermark(col_idx, DataType::Int64, ScalarImpl::Int64(val));
    }
}

impl std::fmt::Debug for MockSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockSource").finish()
    }
}

impl MockSource {
    #[allow(dead_code)]
    pub fn channel() -> (MessageSender, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        let source = Self {
            rx,
            stop_on_finish: true,
        };
        (MessageSender(tx), source)
    }

    #[allow(dead_code)]
    pub fn with_messages(msgs: Vec<Message>) -> Self {
        let (tx, source) = Self::channel();
        for msg in msgs {
            tx.0.send(msg).unwrap();
        }
        source
    }

    pub fn with_chunks(chunks: Vec<StreamChunk>) -> Self {
        let (tx, source) = Self::channel();
        for chunk in chunks {
            tx.0.send(Message::Chunk(chunk)).unwrap();
        }
        source
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn stop_on_finish(self, stop_on_finish: bool) -> Self {
        Self {
            stop_on_finish,
            ..self
        }
    }

    pub fn into_executor(self, schema: Schema, pk_indices: Vec<usize>) -> Executor {
        Executor::new(
            ExecutorInfo {
                schema,
                pk_indices,
                identity: "MockSource".to_owned(),
            },
            self.boxed(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        let mut epoch = test_epoch(1);

        while let Some(msg) = self.rx.recv().await {
            epoch.inc_epoch();
            yield msg;
        }

        if self.stop_on_finish {
            yield Message::Barrier(Barrier::new_test_barrier(epoch).with_stop());
        }
    }
}

impl Execute for MockSource {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
