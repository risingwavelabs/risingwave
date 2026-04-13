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

use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

// --- Request types ---

#[derive(Deserialize)]
pub struct CreateStreamRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct AppendRecordsRequest {
    pub records: Vec<serde_json::Value>,
}

// --- Read request types ---

#[derive(Deserialize)]
pub struct ReadRecordsParams {
    pub after: Option<String>,
    pub limit: Option<u32>,
}

// --- Response types ---

#[derive(Serialize)]
pub struct CreateStreamResponse {
    pub stream: String,
}

#[derive(Serialize)]
pub struct AppendRecordsResponse {
    pub count: usize,
}

#[derive(Serialize)]
pub struct ListStreamsResponse {
    pub streams: Vec<String>,
}

#[derive(Serialize)]
pub struct GetStreamResponse {
    pub name: String,
}

#[derive(Serialize)]
pub struct RecordEntry {
    pub seq_no: String,
    pub body: serde_json::Value,
}

#[derive(Serialize)]
pub struct ReadRecordsResponse {
    pub records: Vec<RecordEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// --- Token response types ---

#[derive(Serialize)]
pub struct CreateTokenResponse {
    pub token: String,
}

#[derive(Serialize)]
pub struct ListTokensResponse {
    pub tokens: Vec<String>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

// --- Error type ---

pub struct RStreamError {
    err: anyhow::Error,
    code: StatusCode,
}

pub type Result<T> = std::result::Result<T, RStreamError>;

pub fn err(err: impl Into<anyhow::Error>, code: StatusCode) -> RStreamError {
    RStreamError {
        err: err.into(),
        code,
    }
}

impl From<anyhow::Error> for RStreamError {
    fn from(value: anyhow::Error) -> Self {
        RStreamError {
            err: value,
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for RStreamError {
    fn into_response(self) -> axum::response::Response {
        let mut resp = Json(ErrorBody {
            error: self.err.to_report_string(),
        })
        .into_response();
        *resp.status_mut() = self.code;
        if self.code == StatusCode::UNAUTHORIZED {
            resp.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                "Bearer".parse().unwrap(),
            );
        }
        resp
    }
}
