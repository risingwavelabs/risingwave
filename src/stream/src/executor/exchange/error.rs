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

use risingwave_rpc_client::error::TonicStatusWrapper;

use crate::task::ActorId;

/// The error type for the exchange channel closed unexpectedly.
///
/// In most cases, this error happens when the upstream or downstream actor
/// exits or panics on other errors, or the network connection is broken.
/// Therefore, this error is usually not the root case of the failure in the
/// streaming graph.
#[derive(Debug)]
pub struct ExchangeChannelClosed {
    message: String,

    /// `Some` if there is a gRPC error from the remote actor.
    source: Option<TonicStatusWrapper>,
}

impl std::fmt::Display for ExchangeChannelClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ExchangeChannelClosed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|s| s as _)
    }

    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        use std::backtrace::Backtrace;

        // Always provide a fake disabled backtrace, so that the upper layer will
        // not capture any other backtraces or include the backtrace in the error
        // log.
        //
        // Otherwise, when an actor exits on a significant error, all connected
        // actor will then exit with the `ExchangeChannelClosed` error, resulting
        // in a very noisy log with flood of useless backtraces.
        static DISABLED_BACKTRACE: Backtrace = Backtrace::disabled();
        request.provide_ref::<Backtrace>(&DISABLED_BACKTRACE);

        if let Some(source) = &self.source {
            source.provide(request);
        }
    }
}

impl ExchangeChannelClosed {
    /// Creates a new error indicating that the exchange channel from the local
    /// upstream actor is closed unexpectedly.
    pub fn local_input(upstream: ActorId) -> Self {
        Self {
            message: format!(
                "exchange channel from local upstream actor {upstream} closed unexpectedly"
            ),
            source: None,
        }
    }

    /// Creates a new error indicating that the exchange channel from the remote
    /// upstream actor is closed unexpectedly, with an optional gRPC error as the cause.
    pub fn remote_input(upstream: ActorId, source: Option<tonic::Status>) -> Self {
        Self {
            message: format!(
                "exchange channel from remote upstream actor {upstream} closed unexpectedly"
            ),
            source: source.map(Into::into),
        }
    }

    /// Creates a new error indicating that the exchange channel to the downstream
    /// actor is closed unexpectedly.
    pub fn output(downstream: ActorId) -> Self {
        Self {
            message: format!(
                "exchange channel to downstream actor {downstream} closed unexpectedly"
            ),
            source: None,
        }
    }
}
