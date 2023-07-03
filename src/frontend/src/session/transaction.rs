use std::sync::{Arc, Weak};

use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use tokio::sync::OnceCell;

use super::SessionImpl;
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::{PinnedHummockSnapshot, PinnedHummockSnapshotRef, SchedulerError};

#[derive(Default)]
pub struct TransactionContext {
    snapshot: Arc<OnceCell<PinnedHummockSnapshotRef>>,
}

#[derive(Default)]
pub enum TransactionState {
    #[default]
    Initial,
    Implicit(TransactionContext),
    Explicit(TransactionContext),
}

impl SessionImpl {
    #[must_use]
    pub fn begin_impicit(&self) -> impl Drop + Send + Sync + 'static {
        let mut txn = self.txn.write();

        match &mut *txn {
            TransactionState::Initial => {
                *txn = TransactionState::Implicit(TransactionContext::default())
            }
            TransactionState::Implicit(_) => unreachable!(),
            TransactionState::Explicit(_) => {}
        }

        struct ResetGuard(Weak<RwLock<TransactionState>>);

        impl Drop for ResetGuard {
            fn drop(&mut self) {
                if let Some(txn) = self.0.upgrade() {
                    let mut txn = txn.write();
                    if let TransactionState::Implicit(_) = &mut *txn {
                        *txn = TransactionState::Initial;
                    }
                }
            }
        }

        ResetGuard(Arc::downgrade(&self.txn))
    }

    pub fn begin_explicit(&self) {
        let mut txn = self.txn.write();

        match &mut *txn {
            TransactionState::Initial => unreachable!(),
            TransactionState::Implicit(ctx) => {
                *txn = TransactionState::Explicit(std::mem::take(ctx))
            }
            TransactionState::Explicit(_) => todo!(),
        }
    }

    pub fn end_explicit(&self) {
        let mut txn = self.txn.write();

        match &mut *txn {
            TransactionState::Initial => unreachable!(),
            TransactionState::Implicit(_) => todo!(),
            TransactionState::Explicit(_ctx) => *txn = TransactionState::Initial,
        }
    }

    fn transaction_ctx(&self) -> MappedRwLockReadGuard<'_, TransactionContext> {
        RwLockReadGuard::map(self.txn.read(), |txn| match txn {
            TransactionState::Initial => unreachable!(),
            TransactionState::Implicit(ctx) => ctx,
            TransactionState::Explicit(ctx) => ctx,
        })
    }

    pub async fn pinned_snapshot(
        &self,
        query_id: &QueryId,
    ) -> Result<PinnedHummockSnapshotRef, SchedulerError> {
        let snapshot = self.transaction_ctx().snapshot.clone();

        snapshot
            .get_or_try_init(|| async move {
                let query_epoch = self.config().get_query_epoch();

                let query_snapshot = if let Some(query_epoch) = query_epoch {
                    PinnedHummockSnapshot::Other(query_epoch)
                } else {
                    // Acquire hummock snapshot for execution.
                    // TODO: if there's no table scan, we don't need to acquire snapshot.
                    let is_barrier_read = self.is_barrier_read();
                    let hummock_snapshot_manager = self.env().hummock_snapshot_manager();
                    let pinned_snapshot = hummock_snapshot_manager.acquire(query_id).await?;
                    PinnedHummockSnapshot::FrontendPinned(pinned_snapshot, is_barrier_read)
                };

                Ok(query_snapshot.into())
            })
            .await
            .cloned()
    }
}
