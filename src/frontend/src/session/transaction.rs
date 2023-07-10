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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use risingwave_common::error::{ErrorCode, Result};
use tokio::sync::OnceCell;

use super::SessionImpl;
use crate::catalog::catalog_service::CatalogWriter;
use crate::scheduler::{PinnedHummockSnapshot, PinnedHummockSnapshotRef, SchedulerResult};
use crate::user::user_service::UserInfoWriter;

/// Globally unique transaction id in this frontend instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(u64);

impl Id {
    /// Creates a new transaction id.
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// Transaction access mode.
// TODO: WriteOnly, CreateDdlOnly
pub enum AccessMode {
    /// Read-write transaction. All operations are permitted.
    ///
    /// Since we cannot handle "read your own writes" in the current implementation, this mode is
    /// only used for single-statement implicit transactions.
    ReadWrite,

    /// Read-only transaction. Only read operations are permitted.
    ///
    /// All reads (except for the system table) are performed on a consistent snapshot acquired at
    /// the first read operation in the transaction.
    ReadOnly,
}

/// Transaction context.
pub struct Context {
    /// The transaction id.
    id: Id,

    /// The access mode of the transaction, defined by the `START TRANSACTION` and the `SET
    /// TRANSACTION` statements
    access_mode: AccessMode,

    /// The snapshot of the transaction, acquired lazily at the first read operation in the
    /// transaction.
    snapshot: Arc<OnceCell<PinnedHummockSnapshotRef>>,
}

/// Transaction state.
// TODO: failed state
#[derive(Default)]
pub enum State {
    /// Initial state, used as a placeholder.
    #[default]
    Initial,

    /// Implicit single-statement transaction.
    ///
    /// Before handling each statement, the session always implicitly starts a transaction with
    /// this state. The state will be reset to `Initial` after the statement is handled unless
    /// the user explicitly starts a transaction with `START TRANSACTION`.
    // TODO: support implicit multi-statement transaction, see [55.2.2.1] Multiple Statements In A
    // Simple Query @ https://www.postgresql.org/docs/15/protocol-flow.html#id-1.10.6.7.4
    Implicit(Context),

    /// Explicit transaction started with `START TRANSACTION`.
    Explicit(Context),
}

/// A guard that auto commits an implicit transaction when dropped. Do nothing if an explicit
/// transaction is in progress.
#[must_use]
pub struct ImplicitAutoCommitGuard(Weak<Mutex<State>>);

impl Drop for ImplicitAutoCommitGuard {
    fn drop(&mut self) {
        if let Some(txn) = self.0.upgrade() {
            let mut txn = txn.lock();
            if let State::Implicit(_) = &*txn {
                *txn = State::Initial;
            }
        }
    }
}

impl SessionImpl {
    /// Starts an implicit transaction if there's no explicit transaction in progress. Called at the
    /// beginning of handling each statement.
    ///
    /// Returns a guard that auto commits the implicit transaction when dropped.
    pub fn txn_begin_implicit(&self) -> ImplicitAutoCommitGuard {
        let mut txn = self.txn.lock();

        match &*txn {
            State::Initial => {
                *txn = State::Implicit(Context {
                    id: Id::new(),
                    access_mode: AccessMode::ReadWrite,
                    snapshot: Default::default(),
                })
            }
            State::Implicit(_) => unreachable!(),
            State::Explicit(_) => {} /* do nothing since an explicit transaction is already in
                                      * progress */
        }

        ImplicitAutoCommitGuard(Arc::downgrade(&self.txn))
    }

    /// Starts an explicit transaction with the specified access mode from `START TRANSACTION`.
    pub fn txn_begin_explicit(&self, access_mode: AccessMode) {
        let mut txn = self.txn.lock();

        match &*txn {
            // Since an implicit transaction is always started, we only need to upgrade it to an
            // explicit transaction.
            State::Initial => unreachable!(),
            State::Implicit(ctx) => {
                *txn = State::Explicit(Context {
                    id: ctx.id,
                    access_mode,
                    snapshot: ctx.snapshot.clone(),
                })
            }
            State::Explicit(_) => {
                // TODO: should be warning
                self.notice_to_user("there is already a transaction in progress")
            }
        }
    }

    /// Commits an explicit transaction.
    // TODO: handle failed transaction
    pub fn txn_commit_explicit(&self) {
        let mut txn = self.txn.lock();

        match &*txn {
            State::Initial => unreachable!(),
            State::Implicit(_) => {
                // TODO: should be warning
                self.notice_to_user("there is no transaction in progress")
            }
            State::Explicit(ctx) => match ctx.access_mode {
                AccessMode::ReadWrite => unimplemented!(),
                AccessMode::ReadOnly => *txn = State::Initial,
            },
        }
    }

    /// Rollbacks an explicit transaction.
    // TODO: handle failed transaction
    pub fn txn_rollback_explicit(&self) {
        let mut txn = self.txn.lock();

        match &*txn {
            State::Initial => unreachable!(),
            State::Implicit(_) => {
                // TODO: should be warning
                self.notice_to_user("there is no transaction in progress")
            }
            State::Explicit(ctx) => match ctx.access_mode {
                AccessMode::ReadWrite => unimplemented!(),
                AccessMode::ReadOnly => *txn = State::Initial,
            },
        }
    }

    /// Returns the transaction context.
    fn txn_ctx(&self) -> MappedMutexGuard<'_, Context> {
        MutexGuard::map(self.txn.lock(), |txn| match txn {
            State::Initial => unreachable!(),
            State::Implicit(ctx) => ctx,
            State::Explicit(ctx) => ctx,
        })
    }

    /// Acquires and pins a snapshot for the current transaction.
    ///
    /// If a snapshot is already acquired, returns it directly.
    pub async fn pinned_snapshot(&self) -> SchedulerResult<PinnedHummockSnapshotRef> {
        let (id, snapshot) = {
            let ctx = self.txn_ctx();
            (ctx.id, ctx.snapshot.clone())
        };

        snapshot
            .get_or_try_init(|| async move {
                let query_epoch = self.config().get_query_epoch();

                let query_snapshot = if let Some(query_epoch) = query_epoch {
                    PinnedHummockSnapshot::Other(query_epoch)
                } else {
                    // Acquire hummock snapshot for execution.
                    let is_barrier_read = self.is_barrier_read();
                    let hummock_snapshot_manager = self.env().hummock_snapshot_manager();
                    let pinned_snapshot = hummock_snapshot_manager.acquire(id).await?;
                    PinnedHummockSnapshot::FrontendPinned(pinned_snapshot, is_barrier_read)
                };

                Ok(query_snapshot.into())
            })
            .await
            .cloned()
    }
}

/// A guard that permits write operations in the current transaction.
///
/// Currently, this is required for [`CatalogWriter`] (including all DDLs), [`UserInfoWriter`]
/// (including `USER` and `GRANT`), and DML operations.
pub struct WriteGuard {
    _private: (),
}

impl SessionImpl {
    /// Returns a [`WriteGuard`], or an error if write operations are not permitted in the current
    /// transaction.
    pub fn txn_write_guard(&self) -> Result<WriteGuard> {
        match self.txn_ctx().access_mode {
            AccessMode::ReadWrite => Ok(WriteGuard { _private: () }),
            AccessMode::ReadOnly => Err(ErrorCode::PermissionDenied(
                "cannot execute in a read-only transaction".into(),
            ))?,
        }
    }

    /// Returns the catalog writer, if write operations are permitted in the current transaction.
    pub fn catalog_writer(&self) -> Result<&dyn CatalogWriter> {
        self.txn_write_guard()
            .map(|guard| self.env().catalog_writer(guard))
    }

    /// Returns the user info writer, if write operations are permitted in the current transaction.
    pub fn user_info_writer(&self) -> Result<&dyn UserInfoWriter> {
        self.txn_write_guard()
            .map(|guard| self.env().user_info_writer(guard))
    }
}
