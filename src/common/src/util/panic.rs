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

#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)]

    use rusty_fork::rusty_fork_test;

    use super::*;

    /// Simulates the behavior of `risingwave_rt::set_panic_hook`.
    fn set_panic_hook() {
        let old = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            old(info);

            if !is_catching_unwind() {
                std::process::abort();
            }
        }))
    }

    rusty_fork_test! {
        #[test]
        #[should_panic] // `rusty_fork` asserts that the forked process succeeds, so this should panic.
        fn test_sync_not_work() {
            set_panic_hook();

            let _result = std::panic::catch_unwind(|| panic!());
        }

        #[test]
        fn test_sync_rw() {
            set_panic_hook();

            let result = rw_catch_unwind(|| panic!());
            assert!(result.is_err());
        }

        #[test]
        fn test_async_rw() {
            set_panic_hook();

            let fut = async { panic!() }.rw_catch_unwind();

            let result = tokio::runtime::Runtime::new().unwrap().block_on(fut);
            assert!(result.is_err());
        }
    }
}
