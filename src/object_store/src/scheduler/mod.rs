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

pub mod count_map;
pub mod range_map;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Range;
use std::rc::Rc;
use std::time::Instant;

use bytes::{BufMut, Bytes, BytesMut};
use count_map::CountMap;
use futures::{Future, FutureExt};
use range_map::{Entry, OrdRange, RangeExt, RangeMap};
use tokio::sync::oneshot;

use crate::object::{ObjectError, ObjectResult};

pub type Sequence = u64;

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
    path: String,

    sequence: Sequence,

    range: Range<usize>,

    enqueue: Instant,

    io_tasks: Vec<IoTask>,

    handles: BTreeMap<OrdRange<usize>, TaskHandle>,
}

impl SchedulerTask {
    pub async fn finish(self) {
        // TODO(MrCroxx): Use `AggregatedBytes` after `Block` can be built from `Buf` to avoid
        // buffer copy.
        let mut buffer = BytesMut::new();
        for (range, handle) in self.handles {
            let range: Range<usize> = range.into();
            match handle.await {
                Err(e) => {
                    for IoTask { tx, .. } in self.io_tasks {
                        tx.send(Err(ObjectError::internal(e.to_string()))).unwrap();
                    }
                    return;
                }
                Ok(data) => {
                    assert_eq!(range.len(), data.len());
                    buffer.put(data);
                }
            }
        }

        let buffer = buffer.freeze();
        let range = self.range;
        assert_eq!(buffer.len(), range.len());
        for IoTask { range: r, tx } in self.io_tasks {
            let start = r.start - range.start;
            let end = r.end - r.start + start;
            let data = buffer.slice(start..end);
            tx.send(Ok(data)).unwrap();
        }
    }
}

pub type SchedulerTaskRef = Rc<RefCell<SchedulerTask>>;

#[derive(Debug)]
pub struct ObjectIoTask {
    path: String,
    range: Range<usize>,
    io_tasks: Vec<IoTask>,
    scheduler_io_task_sequences: Vec<Sequence>,
}

impl ObjectIoTask {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn range(&self) -> &Range<usize> {
        &self.range
    }
}

#[derive(Debug)]
pub struct Launch {
    pub path: String,
    pub ranges: Vec<Range<usize>>,
}

/// [`Scheduler`] guarantees there is no overlapping object io.
#[derive(Debug, Default)]
pub struct Scheduler {
    sequence: Sequence,

    queue: BTreeMap<Sequence, SchedulerTaskRef>,

    /// { path => { range => scheduler task } }
    windows: BTreeMap<String, RangeMap<usize, SchedulerTaskRef>>,

    inflights: BTreeMap<String, RangeMap<usize, ObjectIoTask>>,

    waits: BTreeMap<String, CountMap<Sequence, SchedulerTask>>,
}

impl Scheduler {
    pub fn submit(&mut self, path: &str, range: Range<usize>) -> TaskHandle {
        let sequence = self.sequence;
        self.sequence += 1;
        let (tx, rx) = oneshot::channel();

        let io_task = IoTask {
            range: range.clone(),
            tx,
        };
        let handle = TaskHandle { rx };

        // Join an inflight io task if there is a range covering.
        let inflight = self.inflights.entry(path.to_string()).or_default();
        if let Some(mut covers) = inflight.covers(range.clone()) {
            covers.value_mut().io_tasks.push(io_task);
            return handle;
        }

        let task = Rc::new(RefCell::new(SchedulerTask {
            path: path.to_string(),
            sequence,
            range: range.clone(),
            enqueue: Instant::now(),
            io_tasks: vec![io_task],
            handles: BTreeMap::new(),
        }));

        // Merge into a queued task or join the queue.
        let window = self.windows.entry(path.to_string()).or_default();
        let mut merged_sequences = vec![];
        window.merge(range, {
            let task = task.clone();
            let merged_sequences = &mut merged_sequences;
            move |new_range, merged| {
                for (_r, t) in merged {
                    let mut task = task.borrow_mut();
                    let mut t = t.borrow_mut();

                    merged_sequences.push(t.sequence);
                    task.sequence = task.sequence.min(t.sequence);
                    task.enqueue = task.enqueue.min(t.enqueue);
                    task.io_tasks.append(&mut t.io_tasks);
                }
                task.borrow_mut().range = new_range;
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

    pub fn launch(&mut self) -> Option<Launch> {
        // Pop first scheduler task in queue.
        let Some((_sequence, task)) = self.queue.pop_first() else {
            return None;
        };
        let window = self.windows.get_mut(&task.borrow().path).unwrap();
        window.remove(task.borrow().range.clone()).unwrap();

        let mut task = Rc::into_inner(task).unwrap().into_inner();
        let path = task.path.clone();

        let inflight = self.inflights.entry(path.clone()).or_default();
        let mut split = inflight.split(task.range.clone());

        let mut new_object_io_ranges = vec![];

        while let Some(entry) = split.entry().cloned() {
            // TODO(MrCroxx): split object io by size.
            let (tx, rx) = oneshot::channel();

            let range = match entry {
                Entry::Range(range) => RangeExt::overlaps(&range, &task.range).unwrap(),
                Entry::Gap(range) => range,
            };

            let io_task = IoTask {
                range: range.clone(),
                tx,
            };
            let handle = TaskHandle { rx };
            task.handles.insert(range.clone().into(), handle);

            if let Some(object_io_task) = split.value_mut() {
                object_io_task.io_tasks.push(io_task);
                object_io_task
                    .scheduler_io_task_sequences
                    .push(task.sequence);
                split.skip();
            } else {
                let object_io_task = ObjectIoTask {
                    path: path.clone(),
                    range,
                    io_tasks: vec![io_task],
                    scheduler_io_task_sequences: vec![task.sequence],
                };
                new_object_io_ranges.push(object_io_task.range.clone());
                split.insert(object_io_task);
            }
        }
        drop(split);

        let wait = self.waits.entry(path.clone()).or_default();
        let count = task.handles.len();

        let sequence = task.sequence;
        wait.insert_with_count(sequence, task, count);

        if new_object_io_ranges.is_empty() {
            return None;
        }

        Some(Launch {
            path,
            ranges: new_object_io_ranges,
        })
    }

    pub fn finish(
        &mut self,
        path: &str,
        range: Range<usize>,
        result: ObjectResult<Bytes>,
    ) -> Vec<SchedulerTask> {
        let object_io_task = self.inflights.get_mut(path).unwrap().remove(range).unwrap();
        if let Ok(data) = &result {
            assert_eq!(object_io_task.range.len(), data.len());
        }

        let range = object_io_task.range;

        for IoTask { range: r, tx } in object_io_task.io_tasks {
            let start = r.start - range.start;
            let end = r.end - r.start + start;
            let res = result
                .as_ref()
                .map(|data| data.slice(start..end))
                .map_err(|e| ObjectError::internal(e.to_string()));
            tx.send(res).unwrap();
        }

        let mut tasks = vec![];
        for sequence in object_io_task.scheduler_io_task_sequences {
            if let Some(task) = self.waits.get_mut(path).unwrap().decrease(&sequence) {
                tasks.push(task);
            }
        }

        tasks
    }
}

unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

#[cfg(test)]
mod tests {

    use std::future::{poll_fn, Future};
    use std::pin::{pin, Pin};
    use std::task::Poll::{Pending, Ready};

    use futures::future::join_all;

    use super::*;

    fn ensure_send_sync<T: Send + Sync + 'static>() {}

    async fn assert_pending(handle: &mut Pin<&mut TaskHandle>) {
        assert_eq!(
            Pending,
            poll_fn(|cx| Ready(handle.as_mut().poll(cx)))
                .await
                .map(|res| res.unwrap())
        );
    }

    async fn assert_ready(handle: &mut Pin<&mut TaskHandle>, expected: impl Into<Bytes>) {
        assert_eq!(
            Ready(expected.into()),
            poll_fn(|cx| Ready(handle.as_mut().poll(cx)))
                .await
                .map(|res| res.unwrap())
        );
    }

    #[test]
    fn test_send_sync() {
        ensure_send_sync::<Scheduler>();
    }

    #[tokio::test]
    async fn test_scheduler_simple() {
        let mut s = Scheduler::default();

        let mut h1 = pin!(s.submit("1", 0..10));
        let mut h2 = pin!(s.submit("2", 0..10));
        let mut h3 = pin!(s.submit("1", 5..20));
        let mut h4 = pin!(s.submit("2", 20..30));

        let Launch { path, ranges } = s.launch().unwrap();
        assert_eq!(&path, "1");
        assert_eq!(ranges, vec![0..20]);

        let Launch { path, ranges } = s.launch().unwrap();
        assert_eq!(&path, "2");
        assert_eq!(ranges, vec![0..10]);

        let Launch { path, ranges } = s.launch().unwrap();
        assert_eq!(&path, "2");
        assert_eq!(ranges, vec![20..30]);

        assert!(s.launch().is_none());

        let mut h5 = pin!(s.submit("2", 15..35));

        let Launch { path, ranges } = s.launch().unwrap();
        assert_eq!(&path, "2");
        assert_eq!(ranges, vec![15..20, 30..35]);

        let tasks = s.finish("1", 0..20, Ok(vec![b'x'; 20].into()));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].range, 0..20);

        assert_pending(&mut h1).await;
        assert_pending(&mut h3).await;
        join_all(tasks.into_iter().map(|task| task.finish())).await;
        assert_ready(&mut h1, vec![b'x'; 10]).await;
        assert_ready(&mut h3, vec![b'x'; 15]).await;

        let tasks = s.finish("2", 15..20, Ok(vec![b'x'; 5].into()));
        assert_eq!(tasks.len(), 0);
        let tasks = s.finish("2", 30..35, Ok(vec![b'x'; 5].into()));
        assert_eq!(tasks.len(), 0);
        let tasks = s.finish("2", 20..30, Ok(vec![b'x'; 10].into()));
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].range, 20..30);
        assert_eq!(tasks[1].range, 15..35);

        assert_pending(&mut h4).await;
        assert_pending(&mut h5).await;
        join_all(tasks.into_iter().map(|task| task.finish())).await;
        assert_ready(&mut h4, vec![b'x'; 10]).await;
        assert_ready(&mut h5, vec![b'x'; 20]).await;

        let tasks = s.finish("2", 0..10, Ok(vec![b'x'; 10].into()));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].range, 0..10);

        assert_pending(&mut h2).await;
        join_all(tasks.into_iter().map(|task| task.finish())).await;
        assert_ready(&mut h2, vec![b'x'; 10]).await;
    }
}
