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

pub mod manager;
pub mod report;

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sysinfo::{System, SystemExt};

use crate::util::env_var::env_var_is_true_or;
use crate::util::resource_util::cpu::total_cpu_available;
use crate::util::resource_util::memory::{total_memory_available_bytes, total_memory_used_bytes};

/// Url of telemetry backend
pub const TELEMETRY_REPORT_URL: &str = "https://telemetry.risingwave.dev/api/v1/report";

/// Telemetry reporting interval in seconds, 6 hours
pub const TELEMETRY_REPORT_INTERVAL: u64 = 6 * 60 * 60;

/// Environment Variable that is default to be true
const TELEMETRY_ENV_ENABLE: &str = "ENABLE_TELEMETRY";

pub type TelemetryResult<T> = core::result::Result<T, TelemetryError>;

/// Telemetry errors are generally recoverable/ignorable. `String` is good enough.
pub type TelemetryError = String;

type Result<T> = core::result::Result<T, TelemetryError>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TelemetryNodeType {
    Meta,
    Compute,
    Frontend,
    Compactor,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryReportBase {
    /// tracking_id is persistent in etcd
    pub tracking_id: String,
    /// session_id is reset every time node restarts
    pub session_id: String,
    /// system_data is hardware and os info
    pub system_data: SystemData,
    /// up_time is how long the node has been running
    pub up_time: u64,
    /// time_stamp is when the report is created
    pub time_stamp: u64,
    /// node_type is the node that creates the report
    pub node_type: TelemetryNodeType,
}

pub trait TelemetryReport: Serialize {}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemData {
    memory: Memory,
    os: Os,
    cpu: Cpu,
}

#[derive(Debug, Serialize, Deserialize)]
struct Memory {
    used: usize,
    available: usize,
    total: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Os {
    name: String,
    kernel_version: String,
    version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Cpu {
    // total number of cpu available as a float
    available: f32,
}

impl SystemData {
    pub fn new() -> Self {
        let mut sys = System::new();

        let memory = {
            let available = total_memory_available_bytes();
            let used = total_memory_used_bytes();
            Memory {
                available,
                used,
                total: available + used,
            }
        };

        let os = {
            sys.refresh_system();
            Os {
                name: sys.name().unwrap_or_default(),
                kernel_version: sys.kernel_version().unwrap_or_default(),
                version: sys.os_version().unwrap_or_default(),
            }
        };

        let cpu = Cpu {
            available: total_cpu_available(),
        };

        SystemData { memory, os, cpu }
    }
}

impl Default for SystemData {
    fn default() -> Self {
        Self::new()
    }
}

/// Sends a `POST` request of the telemetry reporting to a URL.
async fn post_telemetry_report(url: &str, report_body: String) -> Result<()> {
    let client = reqwest::Client::new();
    let res = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(report_body)
        .send()
        .await
        .map_err(|err| format!("failed to send telemetry report, err: {}", err))?;
    if res.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "telemetry response is error, url {}, status {}",
            url,
            res.status()
        ))
    }
}

/// check whether telemetry is enabled in environment variable
pub fn telemetry_env_enabled() -> bool {
    // default to be true
    env_var_is_true_or(TELEMETRY_ENV_ENABLE, true)
}

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Clock might go backward")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_data_new() {
        let system_data = SystemData::new();

        assert!(system_data.memory.available > 0);
        assert!(system_data.memory.used > 0);
        assert!(system_data.memory.total > 0);
        assert!(!system_data.os.name.is_empty());
        assert!(!system_data.os.kernel_version.is_empty());
        assert!(!system_data.os.version.is_empty());
        assert!(system_data.cpu.available > 0.0);
    }

    #[test]
    fn test_env() {
        let key = "ENABLE_TELEMETRY";

        // make assertions more readable...
        fn is_enabled() -> bool {
            telemetry_env_enabled()
        }
        fn is_not_enabled() -> bool {
            !is_enabled()
        }

        std::env::set_var(key, "true");
        assert!(is_enabled());

        std::env::set_var(key, "false");
        assert!(is_not_enabled());

        std::env::set_var(key, "tRue");
        assert!(is_enabled());

        std::env::set_var(key, "2");
        assert!(is_not_enabled());

        std::env::set_var(key, "1");
        assert!(is_enabled());

        std::env::set_var(key, "not_a_bool");
        assert!(is_not_enabled());

        std::env::remove_var(key);
        assert!(is_enabled());
    }
}
