// Copyright 2024 RisingWave Labs
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
#![feature(iterator_try_collect)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(more_qualified_paths)]
#![feature(lint_reasons)]
#![feature(let_chains)]
#![feature(hash_extract_if)]
#![feature(extract_if)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(allocator_api)]
#![feature(map_try_insert)]
#![feature(never_type)]
#![feature(btreemap_alloc)]
#![feature(lazy_cell)]
#![feature(error_generic_member_access)]
#![feature(btree_extract_if)]
#![feature(bound_map)]
#![feature(iter_order_by)]
#![feature(exact_size_is_empty)]
#![feature(impl_trait_in_assoc_type)]
#![feature(test)]
#![feature(is_sorted)]
#![feature(btree_cursors)]
#![feature(assert_matches)]
#![feature(try_blocks)]

use std::sync::{Arc, OnceLock};

use risingwave_common::config::StreamingConfig;

#[macro_use]
extern crate tracing;

pub mod cache;
pub mod common;
pub mod error;
pub mod executor;
mod from_proto;
pub mod task;

#[cfg(test)]
risingwave_expr_impl::enable!();

pub(crate) static CONFIG: OnceLock<Arc<StreamingConfig>> = OnceLock::new();

fn streaming_config() -> &'static StreamingConfig {
    CONFIG
        .get()
        .expect("should be set when compute node initializing")
}

mod consistency {
    //! This module contains global variables and methods to access the stream consistency settings.

    use std::sync::LazyLock;

    use risingwave_common::util::env_var::env_var_is_true;

    static INSANE_MODE: LazyLock<bool> =
        LazyLock::new(|| env_var_is_true("RW_UNSAFE_ENABLE_INSANE_MODE"));

    pub(crate) fn insane() -> bool {
        *INSANE_MODE
    }

    pub(crate) fn enable_strict_consistency() -> bool {
        crate::streaming_config().unsafe_enable_strict_consistency
    }

    macro_rules! inconsistency_panic {
        ($($arg:tt)*) => {
            tracing::error!($($arg)*);
            if crate::consistency::enable_strict_consistency() {
                panic!("inconsistency happened, see error log for details");
            }
        };
    }
    pub(crate) use inconsistency_panic;
}
