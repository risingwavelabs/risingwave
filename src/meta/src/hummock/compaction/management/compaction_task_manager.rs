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

use std::collections::HashMap;

use futures::StreamExt;
use risingwave_common::util::select_all;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc::{self, unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::CompactionTaskEvent;

pub struct CompactionTaskManager {
    // compaction_task_rx: mpsc::UnboundedReceiver<(CompactionGroupId, CompactTask, u64)>,
    task_event_sender: mpsc::UnboundedSender<CompactionTaskEvent>,

    task_event_receiver: Option<mpsc::UnboundedReceiver<CompactionTaskEvent>>,

    backend_event_receiver: Option<mpsc::UnboundedReceiver<CompactionTaskEvent>>,

    // compaction_group_id -> <task_id -> task>
    compaction_task: HashMap<CompactionGroupId, HashMap<u64, CompactTask>>,
}

impl CompactionTaskManager {
    fn new(
        // compaction_task_rx: mpsc::UnboundedReceiver<(CompactionGroupId, CompactTask, u64)>,
        backend_event_receiver: mpsc::UnboundedReceiver<CompactionTaskEvent>,
    ) -> Self {
        let (tx, rx) = unbounded_channel();
        Self {
            task_event_sender: tx,
            task_event_receiver: Some(rx),
            backend_event_receiver: Some(backend_event_receiver),

            compaction_task: HashMap::default(),
        }
    }

    pub async fn start_compact_task_event_handler(mut self) {
        let event_trigger = vec![
            Box::pin(UnboundedReceiverStream::new(
                self.task_event_receiver.take().unwrap(),
            )),
            Box::pin(UnboundedReceiverStream::new(
                self.backend_event_receiver.take().unwrap(),
            )),
        ];

        let mut event_stream = select_all(event_trigger);

        while let Some(event) = event_stream.next().await {
            match event {
                CompactionTaskEvent::Register(compaction_group_id, compact_task, timeout) => {
                    self.on_handle_register()
                }

                CompactionTaskEvent::TaskProgress(compact_task) => self.on_handle_task_progress(),

                CompactionTaskEvent::Cancel(compact_task) => self.on_handle_cancel(),

                CompactionTaskEvent::Timer(task_id, timeout) => self.on_handle_timer(),
            }
        }
    }

    fn on_handle_register(&mut self) {
        todo!()
    }

    fn on_handle_task_progress(&mut self) {
        todo!()
    }

    fn on_handle_cancel(&mut self) {
        todo!()
    }

    fn on_handle_timer(&mut self) {
        // TODO handler timeout
        // 1. cancel task
        // 2. User-customized event ?

        todo!()
    }
}
