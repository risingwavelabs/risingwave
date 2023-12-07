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

//! Configures the RisingWave binary, including logging, locks, panic handler, etc.
//!
//! See [`init_risingwave_logger`] and [`main_okk`] for more details, including supported
//! environment variables.

#![feature(panic_update_hook)]
#![feature(let_chains)]

use futures::Future;

mod logger;
pub use logger::*;
mod deadlock;
pub use deadlock::*;
mod panic_hook;
pub use panic_hook::*;
mod prof;
use prof::*;

/// Start RisingWave components with configs from environment variable.
///
/// Currently, the following env variables will be read:
///
/// * `RW_WORKER_THREADS` (alias of `TOKIO_WORKER_THREADS`): number of tokio worker threads. If
///   not set, it will be decided by tokio. Note that this can still be overridden by per-module
///   runtime worker thread settings in the config file.
/// * `RW_DEADLOCK_DETECTION`: whether to enable deadlock detection. If not set, will enable in
///   debug mode, and disable in release mode.
/// * `RW_PROFILE_PATH`: the path to generate flamegraph. If set, then profiling is automatically
///   enabled.
pub fn main_okk<F>(f: F) -> F::Output
where
    F: Future + Send + 'static,
{
    set_panic_hook();

    risingwave_variables::init_server_start_time();

    // `TOKIO` will be read by tokio. Duplicate `RW` for compatibility.
    if let Some(worker_threads) = std::env::var_os("RW_WORKER_THREADS") {
        std::env::set_var("TOKIO_WORKER_THREADS", worker_threads);
    }

    if let Ok(enable_deadlock_detection) = std::env::var("RW_DEADLOCK_DETECTION") {
        let enable_deadlock_detection = enable_deadlock_detection
            .parse()
            .expect("Failed to parse RW_DEADLOCK_DETECTION");
        if enable_deadlock_detection {
            enable_parking_lot_deadlock_detection();
        }
    } else {
        // In case the env variable is not set
        if cfg!(debug_assertions) {
            enable_parking_lot_deadlock_detection();
        }
    }

    if let Ok(profile_path) = std::env::var("RW_PROFILE_PATH") {
        spawn_prof_thread(profile_path);
    }

    tokio::runtime::Builder::new_multi_thread()
        .thread_name("risingwave-main")
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}
