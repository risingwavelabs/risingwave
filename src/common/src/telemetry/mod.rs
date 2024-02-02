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

pub mod manager;
pub mod report;

use std::time::SystemTime;

use risingwave_pb::telemetry::{
    ReportBase as PbTelemetryReportBase, SystemCpu as PbSystemCpu, SystemData as PbSystemData,
    SystemMemory as PbSystemMemory, SystemOs as PbSystemOs,
    TelemetryNodeType as PbTelemetryNodeType,
};
use serde::{Deserialize, Serialize};
use sysinfo::System;
use thiserror_ext::AsReport;

use crate::util::env_var::env_var_is_true_or;
use crate::util::resource_util::cpu::total_cpu_available;
use crate::util::resource_util::memory::{system_memory_available_bytes, total_memory_used_bytes};

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

impl Into<PbTelemetryReportBase> for TelemetryReportBase {
    fn into(self) -> PbTelemetryReportBase {
        PbTelemetryReportBase {
            tracking_id: self.tracking_id,
            session_id: self.session_id,
            system_data: Some(self.system_data.into()),
            up_time: self.up_time,
            report_time: self.time_stamp,
            node_type: from_telemetry_node_type(self.node_type) as i32,
        }
    }
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
        let memory = {
            let total = system_memory_available_bytes();
            let used = total_memory_used_bytes();
            Memory { used, total }
        };

        let os = Os {
            name: System::name().unwrap_or_default(),
            kernel_version: System::kernel_version().unwrap_or_default(),
            version: System::os_version().unwrap_or_default(),
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
        .map_err(|err| format!("failed to send telemetry report, err: {}", err.as_report()))?;
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

fn from_telemetry_node_type(t: TelemetryNodeType) -> PbTelemetryNodeType {
    match t {
        TelemetryNodeType::Meta => PbTelemetryNodeType::Meta,
        TelemetryNodeType::Compute => PbTelemetryNodeType::Compute,
        TelemetryNodeType::Frontend => PbTelemetryNodeType::Frontend,
        TelemetryNodeType::Compactor => PbTelemetryNodeType::Compactor,
    }
}

impl Into<PbTelemetryNodeType> for TelemetryNodeType {
    fn into(self) -> PbTelemetryNodeType {
        match self {
            TelemetryNodeType::Meta => PbTelemetryNodeType::Meta,
            TelemetryNodeType::Compute => PbTelemetryNodeType::Compute,
            TelemetryNodeType::Frontend => PbTelemetryNodeType::Frontend,
            TelemetryNodeType::Compactor => PbTelemetryNodeType::Compactor,
        }
    }
}

impl Into<PbSystemCpu> for Cpu {
    fn into(self) -> PbSystemCpu {
        PbSystemCpu {
            available: self.available,
        }
    }
}

impl Into<PbSystemMemory> for Memory {
    fn into(self) -> PbSystemMemory {
        PbSystemMemory {
            used: self.used as u64,
            total: self.total as u64,
        }
    }
}

impl Into<PbSystemOs> for Os {
    fn into(self) -> PbSystemOs {
        PbSystemOs {
            name: self.name,
            kernel_version: self.kernel_version,
            version: self.version,
        }
    }
}

impl Into<PbSystemData> for SystemData {
    fn into(self) -> PbSystemData {
        PbSystemData {
            memory: Some(self.memory.into()),
            os: Some(self.os.into()),
            cpu: Some(self.cpu.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_data_new() {
        let system_data = SystemData::new();

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
