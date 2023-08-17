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
/// * `RW_WORKER_THREADS`: number of tokio worker threads. If not set, it will use tokio's default
///   config (equivalent to CPU cores). Note: This will not effect the dedicated runtimes for each
///   service which are controlled by their own configurations, like streaming actors, compactions,
///   etc.
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

    let mut builder = tokio::runtime::Builder::new_multi_thread();

    if let Ok(worker_threads) = std::env::var("RW_WORKER_THREADS") {
        let worker_threads = worker_threads.parse().unwrap();
        tracing::info!("setting tokio worker threads to {}", worker_threads);
        builder.worker_threads(worker_threads);
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

    builder
        .thread_name("risingwave-main")
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}
