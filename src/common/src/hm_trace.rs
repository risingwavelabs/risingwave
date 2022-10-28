use bincode::{Decode, Encode};
#[cfg(all(not(madsim), hm_trace))]
use futures::Future;
#[cfg(all(not(madsim), hm_trace))]
use tokio::task::futures::TaskLocalFuture;
#[cfg(all(not(madsim), hm_trace))]
use tokio::task_local;

#[derive(Copy, Clone, PartialEq, Debug, Eq, Decode, Encode)]
pub enum TraceLocalId {
    Actor(u32),
    Executor(u32),
    None,
}

#[cfg(all(not(madsim), hm_trace))]
task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static CONCURRENT_ID: TraceLocalId;
}

#[cfg(all(not(madsim), hm_trace))]
pub fn task_local_scope<F: Future>(
    actor_id: TraceLocalId,
    f: F,
) -> TaskLocalFuture<TraceLocalId, F> {
    CONCURRENT_ID.scope(actor_id, f)
}

#[cfg(all(not(madsim), hm_trace))]
pub fn task_local_get() -> TraceLocalId {
    CONCURRENT_ID.get()
}

#[cfg(madism)]
pub fn task_local_get() -> TraceLocalId {
    TraceLocalId::None
}
