// Copyright 2026 RisingWave Labs
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

use std::sync::OnceLock;

use risingwave_pb::monitor_service::{GetLogFilterRequest, LogFilterState, SetLogFilterRequest};
use thiserror::Error;
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

#[derive(Debug, Error)]
pub enum LogFilterError {
    #[error("invalid log filter: {0}")]
    InvalidFilter(String),
    #[error("runtime log filter control is not initialized")]
    NotInitialized,
    #[error("failed to reload log filter: {0}")]
    Reload(String),
}

impl From<LogFilterError> for Status {
    fn from(error: LogFilterError) -> Self {
        match error {
            LogFilterError::InvalidFilter(_) => Status::invalid_argument(error.to_report_string()),
            LogFilterError::NotInitialized | LogFilterError::Reload(_) => {
                Status::failed_precondition(error.to_report_string())
            }
        }
    }
}

/// Implemented by the runtime logger to keep monitor services independent of it.
pub trait LogFilterControl: Send + Sync {
    fn get(&self) -> LogFilterState;

    /// Replace startup `RUST_LOG` and runtime directives, or restore startup with `None`.
    fn set(&self, filter: Option<&str>) -> Result<LogFilterState, LogFilterError>;
}

static LOG_FILTER_CONTROL: OnceLock<Box<dyn LogFilterControl>> = OnceLock::new();

/// Register the process-wide controller once, during logger initialization.
pub fn set_log_filter_control(control: impl LogFilterControl + 'static) {
    LOG_FILTER_CONTROL
        .set(Box::new(control))
        .ok()
        .expect("log filter control initialized twice");
}

pub fn log_filter_control() -> Result<&'static dyn LogFilterControl, LogFilterError> {
    LOG_FILTER_CONTROL
        .get()
        .map(|control| control.as_ref())
        .ok_or(LogFilterError::NotInitialized)
}

/// Return the process-wide filter state for monitor RPCs.
pub fn get_log_filter_rpc(
    _request: Request<GetLogFilterRequest>,
) -> Result<Response<LogFilterState>, Status> {
    Ok(Response::new(log_filter_control()?.get()))
}

/// Replace the process-wide filter directives, or restore startup when omitted.
pub fn set_log_filter_rpc(
    request: Request<SetLogFilterRequest>,
) -> Result<Response<LogFilterState>, Status> {
    let state = log_filter_control()?.set(request.into_inner().filter.as_deref())?;
    Ok(Response::new(state))
}
