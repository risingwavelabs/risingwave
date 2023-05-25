// Copyright 2023 RisingWave Labs
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

use std::collections::VecDeque;
use std::task::Poll;

use futures::future::{select, Either};
use futures::{Future, FutureExt};
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc;

pub(crate) enum CompactTaskEvent {
    Pending((CompactTask, u64)),
    Committed(CompactTask),
    Finished(CompactTask),
    Cancel(CompactTask),
}

pub(crate) struct NextCompactTaskEvent<'a> {
    pending_task: &'a mut VecDeque<(CompactTask, u64)>,
    committed_task: &'a mut VecDeque<CompactTask>,
    finished_task: &'a mut VecDeque<CompactTask>,
    cancel_task: &'a mut VecDeque<CompactTask>,
}

impl<'a> Future for NextCompactTaskEvent<'a> {
    type Output = CompactTaskEvent;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if !self.cancel_task.is_empty() {
            let task = self.cancel_task.pop_front().unwrap();
            return Poll::Ready(CompactTaskEvent::Cancel(task));
        }

        if !self.finished_task.is_empty() {
            let task = self.finished_task.pop_front().unwrap();
            return Poll::Ready(CompactTaskEvent::Finished(task));
        }

        if !self.committed_task.is_empty() {
            let task = self.committed_task.pop_front().unwrap();
            return Poll::Ready(CompactTaskEvent::Committed(task));
        }

        if !self.pending_task.is_empty() {
            let (task, timeout) = self.pending_task.pop_front().unwrap();
            return Poll::Ready(CompactTaskEvent::Pending((task, timeout)));
        }

        Poll::Pending
    }
}

pub struct CompactionTaskManager {
    compaction_task_rx: mpsc::UnboundedReceiver<(CompactTask, u64)>,

    pub pending_task: VecDeque<(CompactTask, u64)>,
    pub committed_task: VecDeque<CompactTask>,
    pub finished_task: VecDeque<CompactTask>,
    pub cancel_task: VecDeque<CompactTask>,
}

impl CompactionTaskManager {
    fn new(compaction_task_rx: mpsc::UnboundedReceiver<(CompactTask, u64)>) -> Self {
        Self {
            compaction_task_rx,
            pending_task: VecDeque::default(),
            committed_task: VecDeque::default(),
            finished_task: VecDeque::default(),
            cancel_task: VecDeque::default(),
        }
    }

    async fn next_event(&mut self) -> Option<Either<CompactTaskEvent, (CompactTask, u64)>> {
        match select(
            NextCompactTaskEvent {
                pending_task: &mut self.pending_task,
                committed_task: &mut self.committed_task,
                finished_task: &mut self.finished_task,
                cancel_task: &mut self.cancel_task,
            },
            self.compaction_task_rx.recv().boxed(),
        )
        .await
        {
            Either::Left((event, _)) => Some(Either::Left(event)),
            Either::Right((event, _)) => event.map(Either::Right),
        }
    }

    pub async fn start_compact_task_event_handler(mut self) {
        while let Some(event) = self.next_event().await {
            match event {
                Either::Left(event) => match event {
                    CompactTaskEvent::Pending((task, timeout)) => self.on_handle_pending_task(),

                    CompactTaskEvent::Committed(task) => self.on_handle_committed_task(),

                    CompactTaskEvent::Finished(task) => self.on_handle_finished_task(),

                    CompactTaskEvent::Cancel(task) => self.on_handle_cancel_task(),
                },

                Either::Right((compact_task, timeout)) => {
                    self.pending_task.push_back((compact_task, timeout))
                }
            }
        }
    }

    fn on_handle_pending_task(&mut self) {
        todo!()
    }

    fn on_handle_committed_task(&mut self) {
        todo!()
    }

    fn on_handle_finished_task(&mut self) {
        todo!()
    }

    fn on_handle_cancel_task(&mut self) {
        todo!()
    }
}
