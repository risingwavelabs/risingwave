use std::sync::{Arc, Weak};

use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use risingwave_common::error::{ErrorCode, Result};
use tokio::sync::OnceCell;

use super::SessionImpl;
use crate::catalog::catalog_service::CatalogWriter;
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::{PinnedHummockSnapshot, PinnedHummockSnapshotRef, SchedulerResult};
use crate::user::user_service::UserInfoWriter;

#[derive(Default)]
pub enum AccessMode {
    #[default]
    Initial,
    ReadOnly,
    // WriteOnly,
    // DdlOnly,
}

#[derive(Default)]
pub struct Context {
    access_mode: AccessMode,
    snapshot: Arc<OnceCell<PinnedHummockSnapshotRef>>,
}

#[derive(Default)]
pub enum State {
    #[default]
    Initial,
    Implicit(Context),
    Explicit(Context),
}

pub struct WriteGuard {
    _private: (),
}

impl SessionImpl {
    #[must_use]
    pub fn begin_impicit(&self) -> impl Drop + Send + Sync + 'static {
        let mut txn = self.txn.lock();

        match &mut *txn {
            State::Initial => *txn = State::Implicit(Context::default()),
            State::Implicit(_) => unreachable!(),
            State::Explicit(_) => {}
        }

        struct ResetGuard(Weak<Mutex<State>>);

        impl Drop for ResetGuard {
            fn drop(&mut self) {
                if let Some(txn) = self.0.upgrade() {
                    let mut txn = txn.lock();
                    if let State::Implicit(_) = &mut *txn {
                        *txn = State::Initial;
                    }
                }
            }
        }

        ResetGuard(Arc::downgrade(&self.txn))
    }

    pub fn begin_explicit(&self, access_mode: AccessMode) {
        let mut txn = self.txn.lock();

        match &mut *txn {
            State::Initial => unreachable!(),
            State::Implicit(ctx) => {
                *txn = State::Explicit(Context {
                    access_mode,
                    ..std::mem::take(ctx)
                })
            }
            State::Explicit(_) => {
                // TODO: should be warning
                self.notice_to_user("there is already a transaction in progress")
            }
        }
    }

    pub fn end_explicit(&self) {
        let mut txn = self.txn.lock();

        match &mut *txn {
            State::Initial => unreachable!(),
            State::Implicit(_) => {
                // TODO: should be warning
                self.notice_to_user("there is no transaction in progress")
            }
            State::Explicit(_ctx) => *txn = State::Initial,
        }
    }

    fn transaction_ctx(&self) -> MappedMutexGuard<'_, Context> {
        MutexGuard::map(self.txn.lock(), |txn| match txn {
            State::Initial => unreachable!(),
            State::Implicit(ctx) => ctx,
            State::Explicit(ctx) => ctx,
        })
    }

    pub async fn pinned_snapshot(
        &self,
        query_id: &QueryId,
    ) -> SchedulerResult<PinnedHummockSnapshotRef> {
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

    pub fn write_guard(&self) -> Result<WriteGuard> {
        let txn = self.txn.lock();

        let permitted = match &*txn {
            State::Initial => unreachable!(),
            State::Implicit(_) => true,
            State::Explicit(ctx) => match ctx.access_mode {
                AccessMode::Initial => unreachable!(),
                AccessMode::ReadOnly => false,
            },
        };

        if permitted {
            Ok(WriteGuard { _private: () })
        } else {
            Err(ErrorCode::PermissionDenied(
                "cannot execute in a read-only transaction".into(),
            ))?
        }
    }

    pub fn catalog_writer(&self) -> Result<&dyn CatalogWriter> {
        self.write_guard()
            .map(|guard| self.env().catalog_writer(guard))
    }

    pub fn user_info_writer(&self) -> Result<&dyn UserInfoWriter> {
        self.write_guard()
            .map(|guard| self.env().user_info_writer(guard))
    }
}
