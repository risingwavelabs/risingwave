// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::{ErrorCode, RwError, TrackingIssue};
use risingwave_rpc_client::error::RpcError;
use thiserror::Error;

use crate::scheduler::plan_fragmenter::QueryId;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Pin snapshot error: {0} fails to get epoch {1}")]
    PinSnapshot(QueryId, u64),

    #[error("Rpc error: {0}")]
    RpcError(#[from] RpcError),

    #[error("Feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),

    #[error("Empty workers found")]
    EmptyWorkerNodes,

    /// FIXME: include task error msg in this error.
    #[error("Task fail")]
    TaskExecutionError,

    /// Used when receive cancel request (ctrl-c) from user.
    #[error("Canceled by user")]
    QueryCancelError,

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<SchedulerError> for RwError {
    fn from(s: SchedulerError) -> Self {
        ErrorCode::SchedulerError(Box::new(s)).into()
    }
}

impl From<RwError> for SchedulerError {
    fn from(e: RwError) -> Self {
        Self::Internal(e.into())
    }
}
