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
    pub source_rate_limit: Option<u32>,
    pub backfill_rate_limit: Option<u32>,
    pub dml_rate_limit: Option<u32>,
    pub sink_rate_limit: Option<u32>,
}

impl OverwriteOptions {
    pub(crate) const BACKFILL_RATE_LIMIT_KEY: &'static str = "backfill_rate_limit";
    pub(crate) const DML_RATE_LIMIT_KEY: &'static str = "dml_rate_limit";
    pub(crate) const SINK_RATE_LIMIT_KEY: &'static str = "sink_rate_limit";
    pub(crate) const SOURCE_RATE_LIMIT_KEY: &'static str = "source_rate_limit";

    pub fn new(args: &mut HandlerArgs) -> Self {
        let source_rate_limit = {
            if let Some(x) = args.with_options.remove(Self::SOURCE_RATE_LIMIT_KEY) {
                // FIXME(tabVersion): validate the value
                Some(x.parse::<u32>().unwrap())
            } else {
                let rate_limit = args.session.config().source_rate_limit();
                if rate_limit < 0 {
                    None
                } else {
                    Some(rate_limit as u32)
                }
            }
        };
        let backfill_rate_limit = {
            if let Some(x) = args.with_options.remove(Self::BACKFILL_RATE_LIMIT_KEY) {
                // FIXME(tabVersion): validate the value
                Some(x.parse::<u32>().unwrap())
            } else {
                let rate_limit = args.session.config().backfill_rate_limit();
                if rate_limit < 0 {
                    None
                } else {
                    Some(rate_limit as u32)
                }
            }
        };
        let dml_rate_limit = {
            if let Some(x) = args.with_options.remove(Self::DML_RATE_LIMIT_KEY) {
                // FIXME(tabVersion): validate the value
                Some(x.parse::<u32>().unwrap())
            } else {
                let rate_limit = args.session.config().dml_rate_limit();
                if rate_limit < 0 {
                    None
                } else {
                    Some(rate_limit as u32)
                }
            }
        };
        let sink_rate_limit = {
            if let Some(x) = args.with_options.remove(Self::SINK_RATE_LIMIT_KEY) {
                // FIXME(tabVersion): validate the value
                Some(x.parse::<u32>().unwrap())
            } else {
                let rate_limit = args.session.config().sink_rate_limit();
                if rate_limit < 0 {
                    None
                } else {
                    Some(rate_limit as u32)
                }
            }
        };
        Self {
            source_rate_limit,
            backfill_rate_limit,
            dml_rate_limit,
            sink_rate_limit,
        }
    }
}
