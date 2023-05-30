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

//! RisingWave aborts the execution in the panic hook by default to avoid unpredictability and
//! interference in concurrent programming as much as possible. Since the hook is called no matter
//! where the panic occurs, [`std::panic::catch_unwind`] will be a no-op.
//!
//! To workaround this under some circumstances, we provide a task-local flag in practice to
//! indicate whether we're under the context of catching unwind. This is used in the panic hook to
//! decide whether to abort the execution (see the usage of [`is_catching_unwind`]).
//!
//! This module provides several utilities functions wrapping [`std::panic::catch_unwind`] and other
//! related functions to set the flag properly. Calling functions under these contexts will disable
//! the aborting behavior in the panic hook temporarily.

use std::panic::UnwindSafe;

use futures::Future;
use tokio::task::futures::TaskLocalFuture;
use tokio::task_local;

task_local! {
    /// A task-local flag indicating whether we're under the context of catching unwind.
    static CATCH_UNWIND: ()
}

/// Invokes a closure, capturing the cause of an unwinding panic if one occurs.
///
/// See the module-level documentation for why this is needed.
pub fn rw_catch_unwind<F: FnOnce() -> R + UnwindSafe, R>(f: F) -> std::thread::Result<R> {
    CATCH_UNWIND.sync_scope((), || {
        #[expect(clippy::disallowed_methods)]
        std::panic::catch_unwind(f)
    })
}

#[easy_ext::ext(FutureCatchUnwindExt)]
pub impl<F: Future> F {
    /// Catches unwinding panics while polling the future.
    ///
    /// See the module-level documentation for why this is needed.
    fn rw_catch_unwind(self) -> TaskLocalFuture<(), futures::future::CatchUnwind<Self>>
    where
        Self: Sized + std::panic::UnwindSafe,
    {
        CATCH_UNWIND.scope(
            (),
            #[expect(clippy::disallowed_methods)]
            futures::FutureExt::catch_unwind(self),
        )
    }
}

// TODO: extension for `Stream`.

/// Returns whether the current scope is under the context of catching unwind (by calling
/// `rw_catch_unwind`).
pub fn is_catching_unwind() -> bool {
    CATCH_UNWIND.try_with(|_| ()).is_ok()
}
