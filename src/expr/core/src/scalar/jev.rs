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

//! Execution options shared by the Jev binder and expression builder.

use risingwave_common::types::JsonbVal;

use crate::{ExprError, Result};

pub const DEFAULT_MAX_CONCURRENCY: usize = 8;
pub const MAX_CONCURRENCY_LIMIT: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JevEvalOptions {
    pub max_concurrency: usize,
}

impl Default for JevEvalOptions {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }
}

impl JevEvalOptions {
    pub fn parse(options: &JsonbVal) -> Result<Self> {
        let value = options.clone().take();
        let object = value
            .as_object()
            .ok_or_else(|| invalid_options("options must be a JSON object"))?;
        if object.keys().any(|key| key != "max_concurrency") {
            return Err(invalid_options("unknown option; expected max_concurrency"));
        }
        let Some(value) = object.get("max_concurrency") else {
            return Ok(Self::default());
        };
        let max_concurrency = value
            .as_u64()
            .filter(|value| (1..=MAX_CONCURRENCY_LIMIT as u64).contains(value))
            .ok_or_else(|| invalid_options("max_concurrency must be an integer from 1 to 256"))?;
        Ok(Self {
            max_concurrency: max_concurrency as usize,
        })
    }
}

fn invalid_options(reason: &'static str) -> ExprError {
    // Do not include the supplied JSON, including unknown keys, in errors.
    ExprError::InvalidParam {
        name: "jev_eval options",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_bounded_options_without_echoing_values() {
        for (json, expected) in [
            ("{}", 8),
            (r#"{"max_concurrency":1}"#, 1),
            (r#"{"max_concurrency":16}"#, 16),
            (r#"{"max_concurrency":256}"#, 256),
        ] {
            assert_eq!(
                JevEvalOptions::parse(&json.parse().unwrap())
                    .unwrap()
                    .max_concurrency,
                expected
            );
        }
        for json in [
            "null",
            "[]",
            "8",
            r#"{"max_concurrency":null}"#,
            r#"{"max_concurrency":0}"#,
            r#"{"max_concurrency":-1}"#,
            r#"{"max_concurrency":257}"#,
            r#"{"max_concurrency":1.5}"#,
            r#"{"max_concurrency":"test-secret"}"#,
            r#"{"test-secret":8}"#,
        ] {
            let error = JevEvalOptions::parse(&json.parse().unwrap()).unwrap_err();
            assert!(!error.to_string().contains("test-secret"));
        }
    }
}
