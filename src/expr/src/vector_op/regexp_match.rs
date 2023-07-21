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

use std::str::FromStr;

use regex::{Regex, RegexBuilder};
use risingwave_common::array::ListValue;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_expr_macro::function;

use crate::{bail, ExprError, Result};

#[derive(Debug)]
pub struct RegexpContext(pub Regex);

impl RegexpContext {
    pub fn new(pattern: &str, flags: &str) -> Result<Self> {
        let options = RegexpOptions::from_str(flags)?;
        Ok(Self(
            RegexBuilder::new(pattern)
                .case_insensitive(options.case_insensitive)
                .build()?,
        ))
    }

    pub fn from_pattern(pattern: Datum) -> Result<Self> {
        let pattern = match &pattern {
            None => NULL_PATTERN,
            Some(ScalarImpl::Utf8(s)) => s.as_ref(),
            _ => bail!("invalid pattern: {pattern:?}"),
        };
        Self::new(pattern, "")
    }

    pub fn from_pattern_flags(pattern: Datum, flags: Datum) -> Result<Self> {
        let pattern = match (&pattern, &flags) {
            (None, _) | (_, None) => NULL_PATTERN,
            (Some(ScalarImpl::Utf8(s)), _) => s.as_ref(),
            _ => bail!("invalid pattern: {pattern:?}"),
        };
        let flags = match &flags {
            None => "",
            Some(ScalarImpl::Utf8(s)) => s.as_ref(),
            _ => bail!("invalid flags: {flags:?}"),
        };
        Self::new(pattern, flags)
    }
}

/// <https://www.postgresql.org/docs/current/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE>
struct RegexpOptions {
    /// `c` and `i`
    case_insensitive: bool,
}

#[expect(clippy::derivable_impls)]
impl Default for RegexpOptions {
    fn default() -> Self {
        Self {
            case_insensitive: false,
        }
    }
}

impl FromStr for RegexpOptions {
    type Err = ExprError;

    fn from_str(s: &str) -> Result<Self> {
        let mut opts = Self::default();
        for c in s.chars() {
            match c {
                'c' => opts.case_insensitive = false,
                'i' => opts.case_insensitive = true,
                'g' => {}
                _ => {
                    bail!("invalid regular expression option: \"{c}\"");
                }
            }
        }
        Ok(opts)
    }
}

/// The pattern that matches nothing.
pub const NULL_PATTERN: &str = "a^";

#[function(
    "regexp_match(varchar, varchar) -> varchar[]",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_match(varchar, varchar, varchar) -> varchar[]",
    prebuild = "RegexpContext::from_pattern_flags($1, $2)?"
)]
fn regexp_match(text: &str, regex: &RegexpContext) -> Option<ListValue> {
    // If there are multiple captures, then the first one is the whole match, and should be
    // ignored in PostgreSQL's behavior.
    let skip_flag = regex.0.captures_len() > 1;
    let capture = regex.0.captures(text)?;
    let list = capture
        .iter()
        .skip(if skip_flag { 1 } else { 0 })
        .map(|mat| mat.map(|m| m.as_str().into()))
        .collect();
    Some(ListValue::new(list))
}
