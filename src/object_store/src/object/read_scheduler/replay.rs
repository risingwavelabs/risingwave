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

use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{LazyLock, OnceLock};
use std::task::{Context, Wake};

use super::*;

const CAPACITY: usize = 300_000;
type Event = (u64, &'static str, u64, usize, usize);
static EVENTS: OnceLock<Mutex<Vec<Event>>> = OnceLock::new();
static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
static NEXT_PERMIT: AtomicU64 = AtomicU64::new(1);
static NEXT_WAKE: AtomicUsize = AtomicUsize::new(0);
static WAKES: OnceLock<Vec<OnceLock<(u64, u64)>>> = OnceLock::new();

tokio::task_local! { static RESERVATION: u64; }

/// Enable this recorder before constructing the replay object store.
pub fn start() {
    LazyLock::force(&ORIGIN);
    assert!(EVENTS.set(Mutex::new(Vec::with_capacity(CAPACITY))).is_ok());
    assert!(
        WAKES
            .set((0..CAPACITY).map(|_| OnceLock::new()).collect())
            .is_ok()
    );
}

pub fn flush(path: &Path) -> io::Result<()> {
    let mut out = io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(out, "ns\tevent\tid\ta\tb")?;
    let mut events = EVENTS.get().unwrap().lock().unwrap();
    let mut permits = std::collections::HashSet::new();
    for &(at, name, id, a, b) in events.iter() {
        match name {
            "p_acquired" => assert!(permits.insert(id)),
            "p_return_after" => assert!(permits.remove(&id)),
            _ => {}
        }
        writeln!(out, "{at}\t{name}\t{id}\t{a}\t{b}")?;
    }
    assert!(permits.is_empty(), "unreturned replay permits: {permits:?}");
    events.clear();
    drop(events);
    let count = NEXT_WAKE.load(Relaxed);
    assert!(count <= CAPACITY, "replay wake trace overflow");
    for (id, slot) in WAKES.get().unwrap()[..count].iter().enumerate() {
        let (start, end) = *slot.get().unwrap();
        writeln!(out, "{start}\twake_begin\t{id}\t0\t0")?;
        writeln!(out, "{end}\twake_forwarded\t{id}\t0\t0")?;
    }
    out.flush()
}

pub(super) async fn read_scope<T>(id: u64, future: impl Future<Output = T>) -> T {
    if id == 0 {
        return future.await;
    }
    RESERVATION.scope(id, future).await
}

pub(in crate::object) fn backend_attempt() {
    let _ = RESERVATION.try_with(|id| event("attempt_begin", *id, 0, 0));
}

fn now() -> u64 {
    ORIGIN.elapsed().as_nanos() as u64
}

fn push(events: &mut Vec<Event>, name: &'static str, id: u64, a: usize, b: usize) {
    assert!(events.len() < CAPACITY, "replay trace overflow");
    events.push((now(), name, id, a, b));
}

pub(super) fn event(name: &'static str, id: u64, a: usize, b: usize) {
    if let Some(events) = EVENTS.get() {
        push(&mut events.lock().unwrap(), name, id, a, b);
    }
}

pub fn event_marker(name: &'static str) {
    event(name, 0, 0, 0);
}

pub fn depth_sample(admitted: usize, waiting: usize, active: usize) {
    event("depth_sample", active as u64, admitted, waiting);
}

pub(super) fn queue(queue: &PendingReadGroups, reason: &'static str) {
    if EVENTS.get().is_some() {
        let live = queue
            .groups
            .iter()
            .filter(|group| {
                group
                    .group
                    .requests
                    .iter()
                    .any(|request| !request.response.is_closed())
            })
            .count();
        event(reason, 0, live, queue.starting);
    }
}

pub(super) struct PhysicalPermit(Option<OwnedSemaphorePermit>, u64);

impl PhysicalPermit {
    pub(super) fn id(&self) -> u64 {
        self.1
    }

    pub(super) fn backend_begin(&self) {
        if self.id() != 0 {
            event("backend_begin", self.id(), 0, 0);
        }
    }
}

impl Drop for PhysicalPermit {
    fn drop(&mut self) {
        if self.1 != 0
            && let Some(events) = EVENTS.get()
        {
            let permit = self.0.take().unwrap();
            let slots = permit.semaphore().clone();
            let mut events = events.lock().unwrap();
            push(
                &mut events,
                "p_return_before",
                self.1,
                slots.available_permits(),
                0,
            );
            drop(permit);
            push(
                &mut events,
                "p_return_after",
                self.1,
                slots.available_permits(),
                0,
            );
        }
    }
}

struct Acquire<F> {
    future: Option<std::pin::Pin<Box<F>>>,
    slots: Arc<Semaphore>,
    id: u64,
    pending: bool,
}

impl<F: Future<Output = Result<OwnedSemaphorePermit, AcquireError>>> Future for Acquire<F> {
    type Output = Result<PhysicalPermit, AcquireError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut events = EVENTS.get().unwrap().lock().unwrap();
        push(
            &mut events,
            "p_poll",
            this.id,
            this.slots.available_permits(),
            0,
        );
        let result = this.future.as_mut().unwrap().as_mut().poll(cx);
        this.pending = result.is_pending();
        let name = if this.pending {
            "p_pending"
        } else {
            "p_acquired"
        };
        push(
            &mut events,
            name,
            this.id,
            this.slots.available_permits(),
            0,
        );
        result.map(|result| result.map(|permit| PhysicalPermit(Some(permit), this.id)))
    }
}

impl<F> Drop for Acquire<F> {
    fn drop(&mut self) {
        if self.pending {
            let mut events = EVENTS.get().unwrap().lock().unwrap();
            push(
                &mut events,
                "p_abort_before",
                self.id,
                self.slots.available_permits(),
                0,
            );
            drop(self.future.take());
            push(
                &mut events,
                "p_abort_after",
                self.id,
                self.slots.available_permits(),
                0,
            );
        }
    }
}

pub(super) async fn acquire_physical(
    slots: Arc<Semaphore>,
) -> Result<PhysicalPermit, AcquireError> {
    if EVENTS.get().is_none() {
        return slots
            .acquire_owned()
            .await
            .map(|permit| PhysicalPermit(Some(permit), 0));
    }
    Acquire {
        future: Some(Box::pin(slots.clone().acquire_owned())),
        slots,
        id: NEXT_PERMIT.fetch_add(1, Relaxed),
        pending: false,
    }
    .await
}

struct DispatchWake(Waker);
impl Wake for DispatchWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // The semaphore can invoke this while the event mutex is held.
        let start = now();
        self.0.wake_by_ref();
        let end = now();
        let offset = NEXT_WAKE.fetch_add(1, Relaxed);
        if let Some(slot) = WAKES.get().unwrap().get(offset) {
            assert!(slot.set((start, end)).is_ok());
        }
    }
}

pub(super) async fn dispatcher<F: Future>(worker: F) -> F::Output {
    if EVENTS.get().is_none() {
        return worker.await;
    }
    let mut worker = std::pin::pin!(worker);
    let mut waker: Option<(Waker, Waker)> = None;
    let mut poll = 0;
    std::future::poll_fn(|cx| {
        let (parent, wrapped) = waker.get_or_insert_with(|| {
            let parent = cx.waker().clone();
            let wrapped = Waker::from(Arc::new(DispatchWake(parent.clone())));
            (parent, wrapped)
        });
        assert!(parent.will_wake(cx.waker()));
        poll += 1;
        event("dispatcher_poll", poll, 0, 0);
        let result = worker.as_mut().poll(&mut Context::from_waker(wrapped));
        event("dispatcher_exit", poll, 0, 0);
        result
    })
    .await
}
