// Copyright 2023 Singularity Data
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

use anyhow::anyhow;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use sysinfo::{System, SystemExt};

use crate::util::resource_util::cpu::total_cpu_available;
use crate::util::resource_util::memory::{total_memory_available_bytes, total_memory_used_bytes};

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
    let http_client = hyper::Client::new();
    let req = hyper::Request::post(url)
        .header("Content-Type", "application/json")
        .body(hyper::Body::from(report_body))?;

    let res = http_client.request(req).await?;
    if res.status() == StatusCode::OK {
        Ok(())
    } else {
        Err(anyhow!("invalid telemetry resp, status, {}", res.status()))
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use httpmock::MockServer;

    use super::*;

    #[test]
    fn test_system_data() {
        let sys = SystemData::new();
        println!("{:?}", sys);
    }
    #[tokio::test]
    async fn test_post_telemetry_report_success() {
        let mock_server = MockServer::start();
        let url = mock_server.url("/report");

        let report_json = "".to_string();
        let resp_mock = mock_server.mock(|when, then| {
            when.method(POST)
                .path("/report")
                .header("Content-Type", "application/json")
                .body(report_json.clone());
            then.status(200);
        });
        post_telemetry_report(&url, report_json).await.unwrap();
        resp_mock.assert();
    }

    #[tokio::test]
    async fn test_post_telemetry_report_fail() {
        let mock_server = MockServer::start();
        let url = mock_server.url("/report");

        let report_json = "".to_string();
        let resp_mock = mock_server.mock(|when, then| {
            when.method(POST)
                .path("/report")
                .header("Content-Type", "application/json")
                .body(report_json.clone());
            then.status(404);
        });
        assert!(post_telemetry_report(&url, report_json).await.is_err());
        resp_mock.assert();
    }
}
