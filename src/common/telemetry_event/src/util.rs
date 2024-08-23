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

use std::time::SystemTime;

use thiserror_ext::AsReport;

use crate::TelemetryError;

type Result<T> = core::result::Result<T, TelemetryError>;

pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Clock might go backward")
        .as_secs()
}

/// Sends a `POST` request of the telemetry reporting to a URL.
pub async fn post_telemetry_report_pb(url: &str, report_body: Vec<u8>) -> Result<()> {
    let client = reqwest::Client::new();
    let res = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
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
