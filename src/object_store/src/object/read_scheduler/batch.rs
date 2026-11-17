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

//! Briefly plug actual external reads from one prefetch window.
//!
//! The first read schedules one yield before flushing; registration lifetime and
//! cache lookup completion do not control flushing. Late reads use normal admission.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use super::{ReadResult, ReadScheduler, StagedRead};

/// A prefetch window plug. The first external read starts flushing; dropping
/// this handle does not flush. Late reads use ordinary scheduler admission.
pub struct ReadPlug {
    token: ReadPlugToken,
}

/// Shared token captured by an external-read future. Holding or dropping a
/// token does not control when the plug closes.
#[derive(Clone)]
pub struct ReadPlugToken(Arc<ReadPlugInner>);

struct ReadPlugInner {
    scheduler: Arc<ReadScheduler>,
    // Some(empty) is idle; Some(nonempty) is collecting; None is closed.
    reads: Mutex<Option<Vec<StagedRead>>>,
    max_reads: usize,
}

impl ReadPlug {
    pub(in crate::object) fn new(scheduler: Arc<ReadScheduler>, max_reads: usize) -> Self {
        Self {
            token: ReadPlugToken(Arc::new(ReadPlugInner {
                reads: Mutex::new(Some(Vec::new())),
                max_reads: max_reads.min(scheduler.config.max_batch_requests.get()),
                scheduler,
            })),
        }
    }

    pub fn token(&self) -> ReadPlugToken {
        self.token.clone()
    }
}

// Construct this before spawning so cancellation before the first poll also
// closes the plug. A reader on another runtime may still retain its token.
struct PendingFlush(Arc<ReadPlugInner>);

impl Drop for PendingFlush {
    fn drop(&mut self) {
        self.0.reads.lock().unwrap().take();
    }
}

impl ReadPlugInner {
    fn flush(&self) {
        // Take once under the same lock as stage, and submit outside the lock.
        let reads = self.reads.lock().unwrap().take();
        if let Some(reads) = reads
            && !reads.is_empty()
        {
            self.scheduler.plug_flushes[usize::from(reads.len() > 1)].inc();
            self.scheduler.submit_batch(reads);
        }
    }
}

impl ReadPlugToken {
    pub(in crate::object) fn stage(
        &self,
        scheduler: &ReadScheduler,
        path: &str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> Option<oneshot::Receiver<ReadResult>> {
        let plug = &self.0;
        if !std::ptr::eq(scheduler, plug.scheduler.as_ref()) {
            return None;
        }
        let mut state = plug.reads.lock().unwrap();
        let reads = state.as_mut()?;
        if reads.len() >= plug.max_reads {
            return None;
        }
        let (read, receiver) = scheduler.stage_read(path, range, trace_id);
        let first = reads.is_empty();
        reads.push(read);
        let full = reads.len() >= plug.max_reads
            || reads
                .iter()
                .fold(0usize, |bytes, read| bytes.saturating_add(read.range.len()))
                >= scheduler.config.max_read_bytes.get();
        // Close atomically with the capacity check. A later arrival must not
        // extend the plug or exceed its bounds while we submit outside the lock.
        let ready = full.then(|| state.take().unwrap());
        drop(state);
        if let Some(reads) = ready {
            plug.scheduler.plug_flushes[usize::from(reads.len() > 1)].inc();
            plug.scheduler.submit_batch(reads);
        } else if first {
            let flush = PendingFlush(plug.clone());
            scheduler.spawn_background(async move {
                tokio::task::yield_now().await;
                flush.0.flush();
                drop(flush);
            });
        }
        Some(receiver)
    }
}
