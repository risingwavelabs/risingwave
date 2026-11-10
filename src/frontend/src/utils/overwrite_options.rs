// Copyright 2023 RisingWave Labs
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

use crate::WithOptions;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;

/// Some options can be configured both in `WITH` clause and session variables.
/// The config in `WITH` clause has higher priority.
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

    pub fn new(args: &mut HandlerArgs) -> Result<Self> {
        let config = args.session.config();
        let with_options = &mut args.with_options;
        Ok(Self {
            source_rate_limit: Self::take_rate_limit(
                with_options,
                Self::SOURCE_RATE_LIMIT_KEY,
                config.source_rate_limit(),
            )?,
            backfill_rate_limit: Self::take_rate_limit(
                with_options,
                Self::BACKFILL_RATE_LIMIT_KEY,
                config.backfill_rate_limit(),
            )?,
            dml_rate_limit: Self::take_rate_limit(
                with_options,
                Self::DML_RATE_LIMIT_KEY,
                config.dml_rate_limit(),
            )?,
            sink_rate_limit: Self::take_rate_limit(
                with_options,
                Self::SINK_RATE_LIMIT_KEY,
                config.sink_rate_limit(),
            )?,
        })
    }

    /// Takes the rate limit `key` out of the `WITH` clause, falling back to the session variable
    /// where a negative value disables the rate limit.
    fn take_rate_limit(
        with_options: &mut WithOptions,
        key: &str,
        session_value: i32,
    ) -> Result<Option<u32>> {
        match with_options.remove(key) {
            Some(value) => value.parse::<u32>().map(Some).map_err(|_| {
                ErrorCode::InvalidParameterValue(format!(
                    "`{key}` must be a non-negative integer, but got `{value}`"
                ))
                .into()
            }),
            None => Ok(u32::try_from(session_value).ok()),
        }
    }
}
