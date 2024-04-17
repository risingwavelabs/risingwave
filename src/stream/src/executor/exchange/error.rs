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

#[derive(Debug)]
pub struct ExchangeChannelClosed {
    message: String,
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

        static DISABLED_BACKTRACE: Backtrace = Backtrace::disabled();
        request.provide_ref::<Backtrace>(&DISABLED_BACKTRACE);

        if let Some(source) = &self.source {
            source.provide(request);
        }
    }
}

impl ExchangeChannelClosed {
    pub fn local_input(upstream: ActorId) -> Self {
        Self {
            message: format!(
                "exchange channel from local upstream actor {upstream} closed unexpectedly"
            ),
            source: None,
        }
    }

    pub fn remote_input(upstream: ActorId, source: Option<tonic::Status>) -> Self {
        Self {
            message: format!(
                "exchange channel from remote upstream actor {upstream} closed unexpectedly"
            ),
            source: source.map(Into::into),
        }
    }

    pub fn output(downstream: ActorId) -> Self {
        Self {
            message: format!(
                "exchange channel to downstream actor {downstream} closed unexpectedly"
            ),
            source: None,
        }
    }
}
