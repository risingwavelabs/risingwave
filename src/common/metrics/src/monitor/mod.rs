// Copyright 2025 RisingWave Labs
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

pub use connection::{EndpointExt, RouterExt, TcpConfig, monitor_connector};
pub use rwlock::MonitoredRwLock;

mod connection;
pub mod in_mem;
mod process;
mod rwlock;

use std::sync::LazyLock;

use prometheus::Registry;

#[cfg(target_os = "linux")]
static PAGESIZE: std::sync::LazyLock<i64> =
    std::sync::LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) });

#[cfg(target_os = "linux")]
pub static CLOCK_TICK: std::sync::LazyLock<u64> =
    std::sync::LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_CLK_TCK) as u64 });

pub static GLOBAL_METRICS_REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let registry = Registry::new();
    process::monitor_process(&registry);
    registry
});
