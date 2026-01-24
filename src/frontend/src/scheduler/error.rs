// Copyright 2022 RisingWave Labs
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

use risingwave_batch::error::BatchError;
use risingwave_common::session_config::QueryMode;
use risingwave_connector::error::ConnectorError;
use risingwave_rpc_client::error::RpcError;
use thiserror::Error;

use crate::error::{ErrorCode, RwError};
use crate::scheduler::plan_fragmenter::QueryId;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("Pin snapshot error: {0} fails to get epoch {1}")]
    PinSnapshot(QueryId, u64),

    #[error(transparent)]
    RpcError(
        #[from]
        #[backtrace]
        RpcError,
    ),

    #[error("{0}")]
    TaskExecutionError(String),

    #[error("Task got killed because compute node running out of memory")]
    TaskRunningOutOfMemory,

    /// Used when receive cancel request for some reason, such as user cancel or timeout.
    #[error("Query cancelled: {0}")]
    QueryCancelled(String),

    #[error("Reject query: the {0} query number reaches the limit: {1}")]
    QueryReachLimit(QueryMode, u64),

    #[error(transparent)]
    BatchError(
        #[from]
        #[backtrace]
        BatchError,
    ),

    #[error(transparent)]
    Connector(
        #[from]
        #[backtrace]
        ConnectorError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
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
