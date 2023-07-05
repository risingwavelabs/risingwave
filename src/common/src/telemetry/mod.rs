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

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sysinfo::{System, SystemExt};

use crate::util::resource_util::cpu::total_cpu_available;
use crate::util::resource_util::memory::{total_memory_available_bytes, total_memory_used_bytes};

/// Url of telemetry backend
pub const TELEMETRY_REPORT_URL: &str = "https://telemetry.risingwave.dev/api/v1/report";

/// Telemetry reporting interval in seconds, 6 hours
pub const TELEMETRY_REPORT_INTERVAL: u64 = 6 * 60 * 60;

/// Environment Variable that is default to be true
const TELEMETRY_ENV_ENABLE: &str = "ENABLE_TELEMETRY";

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

pub trait TelemetryReport {
    fn to_json(&self) -> Result<String>;
}

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

/// post a telemetry reporting request
pub async fn post_telemetry_report(url: &str, report_body: String) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();
    let res = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(report_body)
        .send()
        .await?;
    if res.status().is_success() {
        Ok(())
    } else {
        Err(anyhow!(
            "invalid telemetry resp, url {}, status {}",
            url,
            res.status()
        ))
    }
}

/// check whether telemetry is enabled in environment variable
pub fn telemetry_env_enabled() -> bool {
    // default to be true
    get_bool_env(TELEMETRY_ENV_ENABLE).unwrap_or(true)
}

pub fn get_bool_env(key: &str) -> Result<bool> {
    let b = std::env::var(key)
        .unwrap_or("true".to_string())
        .trim()
        .to_ascii_lowercase()
        .parse()?;
    Ok(b)
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
    fn test_get_bool_env_true() {
        let key = "MY_ENV_VARIABLE_TRUE";
        std::env::set_var(key, "true");
        let result = get_bool_env(key).unwrap();
        assert!(result);
    }

    #[test]
    fn test_get_bool_env_false() {
        let key = "MY_ENV_VARIABLE_FALSE";
        std::env::set_var(key, "false");
        let result = get_bool_env(key).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_get_bool_env_default() {
        let key = "MY_ENV_VARIABLE_NOT_SET";
        std::env::remove_var(key);
        let result = get_bool_env(key).unwrap();
        assert!(result);
    }

    #[test]
    fn test_get_bool_env_case_insensitive() {
        let key = "MY_ENV_VARIABLE_MIXED_CASE";
        std::env::set_var(key, "tRue");
        let result = get_bool_env(key).unwrap();
        assert!(result);
    }

    #[test]
    fn test_get_bool_env_invalid() {
        let key = "MY_ENV_VARIABLE_INVALID";
        std::env::set_var(key, "not_a_bool");
        let result = get_bool_env(key);
        assert!(result.is_err());
    }
}
