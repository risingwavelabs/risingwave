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

use crate::handler::HandlerArgs;

#[derive(Debug, Clone, Default)]
pub struct OverwriteOptions {
    pub streaming_rate_limit: Option<u32>,
}

impl OverwriteOptions {
    const STREAMING_RATE_LIMIT_KEY: &'static str = "streaming_rate_limit";

    pub fn new(args: &mut HandlerArgs) -> Self {
        let streaming_rate_limit = {
            if let Some(x) = args
                .with_options
                .inner_mut()
                .remove(Self::STREAMING_RATE_LIMIT_KEY)
            {
                // FIXME(tabVersion): validate the value
                Some(x.parse::<u32>().unwrap())
            } else {
                args.session
                    .config()
                    .streaming_rate_limit()
                    .map(|limit| limit.get() as u32)
            }
        };
        Self {
            streaming_rate_limit,
        }
    }
}
