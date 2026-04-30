// Copyright 2022 RisingWave Labs
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

#![allow(clippy::derive_partial_eq_without_eq)]
#![recursion_limit = "256"]
#![feature(iterator_try_collect)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(more_qualified_paths)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(allocator_api)]
#![feature(map_try_insert)]
#![feature(never_type)]
#![feature(btreemap_alloc)]
#![feature(error_generic_member_access)]
#![feature(iter_order_by)]
#![feature(exact_size_is_empty)]
#![feature(impl_trait_in_assoc_type)]
#![feature(test)]
#![feature(btree_cursors)]
#![feature(assert_matches)]
#![feature(try_blocks)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use risingwave_common::config::StreamingConfig;

#[macro_use]
extern crate tracing;

pub mod cache;
pub mod common;
pub mod error;
pub mod executor;
mod from_proto;
pub mod task;
pub mod telemetry;

#[cfg(test)]
risingwave_expr_impl::enable!();

tokio::task_local! {
    pub(crate) static CONFIG: Arc<StreamingConfig>;
    /// True iff this actor owns at least one state table with TTL; consistency macros stay silent.
    pub(crate) static ACTOR_HAS_TTL_STATE: Arc<AtomicBool>;
}

mod config {
    use risingwave_common::config::streaming::default;

    pub(crate) fn chunk_size() -> usize {
        let res = crate::CONFIG.try_with(|config| config.developer.chunk_size);
        if res.is_err() && cfg!(not(test)) {
            tracing::warn!("streaming CONFIG is not set, which is probably a bug")
        }
        res.unwrap_or_else(|_| default::developer::stream_chunk_size())
    }
}

mod consistency {
    //! This module contains global variables and methods to access the stream consistency settings.

    use std::sync::LazyLock;

    use risingwave_common::config::streaming::default;
    use risingwave_common::util::env_var::env_var_is_true;

    static INSANE_MODE: LazyLock<bool> =
        LazyLock::new(|| env_var_is_true("RW_UNSAFE_ENABLE_INSANE_MODE"));

    /// Check if the insane mode is enabled.
    pub(crate) fn insane() -> bool {
        *INSANE_MODE
    }

    pub(crate) fn actor_has_ttl_state() -> bool {
        crate::ACTOR_HAS_TTL_STATE
            .try_with(|f| f.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(false)
    }

    /// Check if strict consistency is required.
    pub(crate) fn enable_strict_consistency() -> bool {
        if actor_has_ttl_state() {
            return false;
        }
        let res = crate::CONFIG.try_with(|config| !config.unsafe_disable_strict_consistency);
        if res.is_err() && cfg!(not(test)) {
            tracing::warn!("streaming CONFIG is not set, which is probably a bug");
        }
        res.unwrap_or_else(|_| !default::streaming::unsafe_disable_strict_consistency())
    }

    /// Log an error message for breaking consistency. Must only be called in non-strict mode.
    /// The log message will be suppressed if it is called too frequently.
    macro_rules! consistency_error {
        ($($arg:tt)*) => {
            if !crate::consistency::actor_has_ttl_state() {
                debug_assert!(!crate::consistency::enable_strict_consistency());

                use std::sync::LazyLock;
                use risingwave_common::log::LogSuppressor;

                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::error!(suppressed_count, $($arg)*);
                }
            }
        };
    }
    pub(crate) use consistency_error;

    /// Log an error message for breaking consistency, then panic if strict consistency is required.
    /// The log message will be suppressed if it is called too frequently.
    macro_rules! consistency_panic {
        ($($arg:tt)*) => {
            if crate::consistency::enable_strict_consistency() {
                tracing::error!($($arg)*);
                panic!("inconsistency happened, see error log for details");
            } else {
                crate::consistency::consistency_error!($($arg)*);
            }
        };
    }
    pub(crate) use consistency_panic;

    /// Like `assert!`, but routes failure through `consistency_panic!` (panic in strict mode,
    /// log in non-strict). The side effect after the assertion still runs in non-strict mode,
    /// so use only at sites where the post-assertion behavior is sane on a violated invariant
    /// (e.g. last-write-wins).
    macro_rules! consistent_assert {
        ($cond:expr, $($arg:tt)*) => {
            if !($cond) {
                crate::consistency::consistency_panic!($($arg)*);
            }
        };
    }
    pub(crate) use consistent_assert;

    /// Saturating-at-zero decrement that routes underflow through `consistency_panic!`.
    pub(crate) trait ConsistentCounter: Sized {
        /// Decrement by 1. Returns `false` if saturated; in that case the value is
        /// left at 0 and a `consistency_panic!` is raised.
        fn consistent_dec(&mut self, msg: &'static str) -> bool;
    }

    impl ConsistentCounter for u64 {
        #[track_caller]
        fn consistent_dec(&mut self, msg: &'static str) -> bool {
            if *self == 0 {
                consistency_panic!("{}", msg);
                return false;
            }
            *self -= 1;
            true
        }
    }

    impl ConsistentCounter for i64 {
        #[track_caller]
        fn consistent_dec(&mut self, msg: &'static str) -> bool {
            if *self <= 0 {
                consistency_panic!(prev = *self, "{}", msg);
                *self = 0;
                return false;
            }
            *self -= 1;
            true
        }
    }

    impl ConsistentCounter for usize {
        #[track_caller]
        fn consistent_dec(&mut self, msg: &'static str) -> bool {
            if *self == 0 {
                consistency_panic!("{}", msg);
                return false;
            }
            *self -= 1;
            true
        }
    }
}
