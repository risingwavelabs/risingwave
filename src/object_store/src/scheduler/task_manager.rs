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

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Range;
use std::rc::Rc;
use std::time::Instant;

use bytes::Bytes;
use futures::{Future, FutureExt};
use tokio::sync::oneshot;

use super::range_map::RangeMap;
use crate::object::{ObjectError, ObjectResult};

pub type Sequence = u64;

#[derive(Debug)]
pub struct Task {
    range: Range<usize>,
}

#[derive(Debug)]
pub struct TaskHandle {
    rx: oneshot::Receiver<ObjectResult<Bytes>>,
}

impl Future for TaskHandle {
    type Output = ObjectResult<Bytes>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.rx
            .poll_unpin(cx)
            .map(|res| res.map_err(ObjectError::internal).and_then(|r| r))
    }
}

#[derive(Debug)]
pub struct IoTask {
    range: Range<usize>,
    tx: oneshot::Sender<ObjectResult<Bytes>>,
}

#[derive(Debug)]
pub struct SchedulerTask {
    sequence: Sequence,

    range: Range<usize>,

    enqueue: Instant,

    ios: Vec<IoTask>,
}

pub type SchedulerTaskRef = Rc<RefCell<SchedulerTask>>;

#[derive(Debug, Default)]
pub struct TaskManager {
    sequence: Sequence,

    queue: BTreeMap<Sequence, SchedulerTaskRef>,
    window: RangeMap<usize, SchedulerTaskRef>,

    inflight: RangeMap<usize, SchedulerTask>,

    ios: RangeMap<usize, IoTask>,
}

impl TaskManager {
    pub fn submit(&mut self, task: Task) -> TaskHandle {
        let range = task.range;
        let (tx, rx) = oneshot::channel();
        let handle = TaskHandle { rx };

        // Join a inflight task if there is a range covering.
        if let Some(mut covers) = self.inflight.covers(range.clone()) {
            covers.value_mut().ios.push(IoTask { range, tx });
            return handle;
        }

        // Merge into a queued task or join the queue.
        let task = Rc::new(RefCell::new(SchedulerTask {
            sequence: self.sequence,
            range: 0..0,
            enqueue: Instant::now(),
            ios: vec![],
        }));
        self.sequence += 1;

        let mut merged_sequences = vec![];
        self.window.merge(range.clone(), {
            let task = task.clone();
            let merged_sequences = &mut merged_sequences;
            move |new_range, merged| {
                for (_r, t) in merged {
                    merged_sequences.push(t.borrow().sequence);
                    task.borrow_mut().sequence = task.borrow().sequence.min(t.borrow().sequence);
                    task.borrow_mut().enqueue = task.borrow().enqueue.min(t.borrow().enqueue);
                    task.borrow_mut().ios.append(&mut t.borrow_mut().ios);
                }
                task.borrow_mut().range = new_range;
                task.borrow_mut().ios.push(IoTask { range, tx });
                task
            }
        });

        for sequence in merged_sequences {
            self.queue.remove(&sequence).unwrap();
        }
        let sequence = task.borrow().sequence;
        self.queue.insert(sequence, task);

        handle
    }

    pub fn launch(&mut self) -> Vec<IoTask> {
        // Pop first scheduler task in queue.
        let Some((_sequence, task)) = self.queue.pop_first() else {
            return vec![];
        };
        self.window.remove(task.borrow().range.clone()).unwrap();
        let task = Rc::into_inner(task).unwrap().into_inner();

        todo!()
    }
}

unsafe impl Send for TaskManager {}
unsafe impl Sync for TaskManager {}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_send_sync<T: Send + Sync + 'static>() {}

    impl TaskManager {
        fn assert_consistency(&self) {
            assert_eq!(self.queue.len(), self.window.len());
            for (sequence, task) in &self.queue {
                let t = self.window.get(task.borrow().range.clone()).unwrap();
                assert_eq!(*sequence, t.borrow().sequence);
            }
        }
    }

    #[test]
    fn test_send_sync() {
        ensure_send_sync::<TaskManager>();
    }

    #[test]
    fn test_task_manager_simple() {
        let mut tm = TaskManager::default();
    }
}
