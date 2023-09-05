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
pub struct RegexpContext {
    pub regex: Regex,
    pub global: bool,
}

impl RegexpContext {
    pub fn new(pattern: &str, flags: &str) -> Result<Self> {
        let options = RegexpOptions::from_str(flags)?;
        Ok(Self {
            regex: RegexBuilder::new(pattern)
                .case_insensitive(options.case_insensitive)
                .build()?,
            global: options.global,
        })
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
#[derive(Default, Debug)]
struct RegexpOptions {
    /// `c` and `i`
    case_insensitive: bool,
    /// `g`
    global: bool,
}

impl FromStr for RegexpOptions {
    type Err = ExprError;

    fn from_str(s: &str) -> Result<Self> {
        let mut opts = Self::default();
        for c in s.chars() {
            match c {
                // Case sensitive matching here
                'c' => opts.case_insensitive = false,
                // Case insensitive matching here
                'i' => opts.case_insensitive = true,
                // Global matching here
                'g' => opts.global = true,
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
    let skip_flag = regex.regex.captures_len() > 1;
    let capture = regex.regex.captures(text)?;
    let list = capture
        .iter()
        .skip(if skip_flag { 1 } else { 0 })
        .map(|mat| mat.map(|m| m.as_str().into()))
        .collect();
    Some(ListValue::new(list))
}

#[function(
    "regexp_count(varchar, varchar) -> int32",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_count(varchar, varchar, int32) -> int32",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_count(varchar, varchar, int32, varchar) -> int32",
    prebuild = "RegexpContext::from_pattern_flags($1, $3)?"
)]
fn regexp_count(text: &str, start: i32, regex: &RegexpContext) -> i32 {
    // First get the start position to count for
    let start = if let Some(s) = self.start { s - 1 } else { 0 };

    // For unicode purpose
    let mut start = match text.char_indices().nth(start as usize) {
        Some((idx, _)) => idx,
        // The `start` is out of bound
        None => return Some(0),
    };

    let mut count = 0;
    while let Some(captures) = self.ctx.regex.captures(&text[start..]) {
        count += 1;
        start += captures.get(0).unwrap().end();
    }
    count
}

#[function(
    "regexp_replace(varchar, varchar, varchar) -> varchar",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_replace(varchar, varchar, varchar, int32) -> varchar",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_replace(varchar, varchar, varchar, int32, int32) -> varchar",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_replace(varchar, varchar, varchar, int32, int32, varchar) -> varchar",
    prebuild = "RegexpContext::from_pattern_flags($1, $5)?"
)]
fn regexp_replace(
    text: &str,
    replacement: &str,
    start: i32,
    n: i32,
    ctx: &RegexpContext,
) -> Option<ListValue> {
    // The start position to begin the search
    let start = if let Some(s) = start { s - 1 } else { 0 };

    // This is because the source text may contain unicode
    let start = match text.char_indices().nth(start as usize) {
        Some((idx, _)) => idx,
        // With no match
        None => return Some(text.into()),
    };

    if (n.is_none() && ctx.global) || (n.is_some() && n.unwrap() == 0) {
        // --------------------------------------------------------------
        // `-g` enabled (& `N` is not specified) or `N` is `0`          |
        // We need to replace all the occurrence of the matched pattern |
        // --------------------------------------------------------------

        // See if there is capture group or not
        if ctx.regex.captures_len() <= 1 {
            // There is no capture groups in the regex
            // Just replace all matched patterns after `start`
            return Some(
                text[..start].to_string()
                    + &self
                        .ctx
                        .regex
                        .replace_all(&text[start..], replacement.clone()),
            );
        } else {
            // The position to start searching for replacement
            let mut search_start = start;

            // Construct the return string
            let mut ret = text[..search_start].to_string();

            // Begin the actual replace logic
            while let Some(capture) = ctx.regex.captures(&text[search_start..]) {
                let match_start = capture.get(0).unwrap().start();
                let match_end = capture.get(0).unwrap().end();

                if match_start == match_end {
                    // If this is an empty match
                    search_start += 1;
                    continue;
                }

                // Append the portion of the text from `search_start` to `match_start`
                ret.push_str(&text[search_start..search_start + match_start]);

                // Start to replacing
                // Note that the result will be written directly to `ret` buffer
                capture.expand(&replacement, &mut ret);

                // Update the `search_start`
                search_start += match_end;
            }

            // Push the rest of the text to return string
            ret.push_str(&text[search_start..]);

            Some(ret)
        }
    } else {
        // -------------------------------------------------
        // Only replace the first matched pattern          |
        // Or the N-th matched pattern if `N` is specified |
        // -------------------------------------------------

        // Construct the return string
        let mut ret = if start > 1 {
            text[..start].to_string()
        } else {
            "".to_string()
        };

        // See if there is capture group or not
        if ctx.regex.captures_len() <= 1 {
            // There is no capture groups in the regex
            if n.is_none() {
                // `N` is not specified
                ret.push_str(&self.ctx.regex.replacen(&text[start..], 1, &replacement));
            } else {
                // Replace only the N-th match
                let mut count = 1;
                // The absolute index for the start of searching
                let mut search_start = start;
                while let Some(capture) = ctx.regex.captures(&text[search_start..]) {
                    // Get the current start & end index
                    let match_start = capture.get(0).unwrap().start();
                    let match_end = capture.get(0).unwrap().end();

                    if count == n.unwrap() as i32 {
                        // We've reached the pattern to replace
                        // Let's construct the return string
                        ret = format!(
                            "{}{}{}",
                            &text[..search_start + match_start],
                            &replacement,
                            &text[search_start + match_end..]
                        );
                        break;
                    }

                    // Update the counter
                    count += 1;

                    // Update `start`
                    search_start += match_end;
                }
            }
        } else {
            // There are capture groups in the regex
            // Reset return string at the beginning
            ret = "".to_string();
            if n.is_none() {
                // `N` is not specified
                if ctx.regex.captures(&text[start..]).is_none() {
                    // No match
                    return Some(text.into());
                }
                // Otherwise replace the source text
                if let Some(capture) = ctx.regex.captures(&text[start..]) {
                    let match_start = capture.get(0).unwrap().start();
                    let match_end = capture.get(0).unwrap().end();

                    // Get the replaced string and expand it
                    capture.expand(&replacement, &mut ret);

                    // Construct the return string
                    ret = format!(
                        "{}{}{}",
                        &text[..start + match_start],
                        ret,
                        &text[start + match_end..]
                    );
                }
            } else {
                // Replace only the N-th match
                let mut count = 1;
                while let Some(capture) = ctx.regex.captures(&text[start..]) {
                    if count == n.unwrap() as i32 {
                        // We've reached the pattern to replace
                        let match_start = capture.get(0).unwrap().start();
                        let match_end = capture.get(0).unwrap().end();

                        // Get the replaced string and expand it
                        capture.expand(&replacement, &mut ret);

                        // Construct the return string
                        ret = format!(
                            "{}{}{}",
                            &text[..start + match_start],
                            ret,
                            &text[start + match_end..]
                        );
                    }

                    // Update the counter
                    count += 1;
                }

                // If there is no match, just return the original string
                if ret.is_empty() {
                    ret = text.into();
                }
            }
        }

        Some(ret)
    }
}
