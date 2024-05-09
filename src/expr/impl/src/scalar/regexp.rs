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

//! Regular expression functions.

use std::str::FromStr;

use fancy_regex::{Regex, RegexBuilder};
use risingwave_common::array::{ArrayBuilder, ListValue, Utf8Array, Utf8ArrayBuilder};
use risingwave_expr::{bail, function, ExprError, Result};
use thiserror_ext::AsReport;

#[derive(Debug)]
pub struct RegexpContext {
    pub regex: Regex,
    pub global: bool,
    pub replacement: String,
}

impl RegexpContext {
    fn new(pattern: &str, flags: &str, replacement: &str) -> Result<Self> {
        let options = RegexpOptions::from_str(flags)?;

        let origin = if options.case_insensitive {
            format!("(?i:{})", pattern)
        } else {
            pattern.to_string()
        };

        let limit = std::env::var("RW_REGEX_BACKTRACK_LIMIT")
            .unwrap_or("1000000".into())
            .parse()
            .unwrap();
        Ok(Self {
            regex: RegexBuilder::new(&origin)
                .backtrack_limit(limit)
                .build()
                .map_err(|e| ExprError::Parse(e.to_report_string().into()))?,
            global: options.global,
            replacement: make_replacement(replacement),
        })
    }

    pub fn from_pattern(pattern: &str) -> Result<Self> {
        Self::new(pattern, "", "")
    }

    pub fn from_pattern_flags(pattern: &str, flags: &str) -> Result<Self> {
        Self::new(pattern, flags, "")
    }

    pub fn from_pattern_flags_for_count(pattern: &str, flags: &str) -> Result<Self> {
        if flags.contains('g') {
            bail!("regexp_count() does not support the global option");
        }
        Self::new(pattern, flags, "")
    }

    pub fn from_pattern_replacement(pattern: &str, replacement: &str) -> Result<Self> {
        Self::new(pattern, "", replacement)
    }

    pub fn from_pattern_replacement_flags(
        pattern: &str,
        replacement: &str,
        flags: &str,
    ) -> Result<Self> {
        Self::new(pattern, flags, replacement)
    }
}

/// Construct the regex used to match and replace `\n` expression.
/// <https://docs.rs/regex/latest/regex/struct.Captures.html#method.expand>
///
/// ```text
/// \& -> ${0}
/// \1 -> ${1}
/// ...
/// \9 -> ${9}
/// ```
fn make_replacement(s: &str) -> String {
    use std::fmt::Write;
    let mut ret = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            ret.push(c);
            continue;
        }
        match chars.next() {
            Some('&') => ret.push_str("${0}"),
            Some(c @ '1'..='9') => write!(&mut ret, "${{{c}}}").unwrap(),
            Some(c) => write!(ret, "\\{c}").unwrap(),
            None => ret.push('\\'),
        }
    }
    ret
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

#[function(
    // source ~ pattern
    "regexp_eq(varchar, varchar) -> boolean",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
fn regexp_eq(text: &str, regex: &RegexpContext) -> bool {
    regex.regex.is_match(text).unwrap()
}

#[function(
    // regexp_match(source, pattern)
    "regexp_match(varchar, varchar) -> varchar[]",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    // regexp_match(source, pattern, flags)
    "regexp_match(varchar, varchar, varchar) -> varchar[]",
    prebuild = "RegexpContext::from_pattern_flags($1, $2)?"
)]
fn regexp_match(text: &str, regex: &RegexpContext) -> Option<ListValue> {
    // If there are multiple captures, then the first one is the whole match, and should be
    // ignored in PostgreSQL's behavior.
    let skip_first = regex.regex.captures_len() > 1;
    let capture = regex.regex.captures(text).unwrap()?;
    let list = capture
        .iter()
        .skip(if skip_first { 1 } else { 0 })
        .map(|mat| mat.map(|m| m.as_str()))
        .collect::<Utf8Array>();
    Some(ListValue::new(list.into()))
}

#[function(
    // regexp_count(source, pattern)
    "regexp_count(varchar, varchar) -> int4",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
fn regexp_count_start0(text: &str, regex: &RegexpContext) -> Result<i32> {
    regexp_count(text, 1, regex)
}

#[function(
    // regexp_count(source, pattern, start)
    "regexp_count(varchar, varchar, int4) -> int4",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    // regexp_count(source, pattern, start, flags)
    "regexp_count(varchar, varchar, int4, varchar) -> int4",
    prebuild = "RegexpContext::from_pattern_flags_for_count($1, $3)?"
)]
fn regexp_count(text: &str, start: i32, regex: &RegexpContext) -> Result<i32> {
    // First get the start position to count for
    let start = match start {
        ..=0 => {
            return Err(ExprError::InvalidParam {
                name: "start",
                reason: start.to_string().into(),
            })
        }
        _ => start as usize - 1,
    };

    // Find the start byte index considering the unicode
    let mut start = match text.char_indices().nth(start) {
        Some((idx, _)) => idx,
        // The `start` is out of bound
        None => return Ok(0),
    };

    let mut count = 0;
    while let Ok(Some(captures)) = regex.regex.captures(&text[start..]) {
        count += 1;
        start += captures.get(0).unwrap().end();
    }
    Ok(count)
}

#[function(
    // regexp_replace(source, pattern, replacement)
    "regexp_replace(varchar, varchar, varchar) -> varchar",
    prebuild = "RegexpContext::from_pattern_replacement($1, $2)?"
)]
#[function(
    // regexp_replace(source, pattern, replacement, flags)
    "regexp_replace(varchar, varchar, varchar, varchar) -> varchar",
    prebuild = "RegexpContext::from_pattern_replacement_flags($1, $2, $3)?"
)]
fn regexp_replace0(text: &str, ctx: &RegexpContext) -> Result<Box<str>> {
    regexp_replace(text, 1, None, ctx)
}

#[function(
    // regexp_replace(source, pattern, replacement, start)
    "regexp_replace(varchar, varchar, varchar, int4) -> varchar",
    prebuild = "RegexpContext::from_pattern_replacement($1, $2)?"
)]
fn regexp_replace_with_start(text: &str, start: i32, ctx: &RegexpContext) -> Result<Box<str>> {
    regexp_replace(text, start, None, ctx)
}

#[function(
    // regexp_replace(source, pattern, replacement, start, N)
    "regexp_replace(varchar, varchar, varchar, int4, int4) -> varchar",
    prebuild = "RegexpContext::from_pattern_replacement($1, $2)?"
)]
fn regexp_replace_with_start_n(
    text: &str,
    start: i32,
    n: i32,
    ctx: &RegexpContext,
) -> Result<Box<str>> {
    regexp_replace(text, start, Some(n), ctx)
}

#[function(
    // regexp_replace(source, pattern, replacement, start, N, flags)
    "regexp_replace(varchar, varchar, varchar, int4, int4, varchar) -> varchar",
    prebuild = "RegexpContext::from_pattern_replacement_flags($1, $2, $5)?"
)]
fn regexp_replace_with_start_n_flags(
    text: &str,
    start: i32,
    n: i32,
    ctx: &RegexpContext,
) -> Result<Box<str>> {
    regexp_replace(text, start, Some(n), ctx)
}

// regexp_replace(source, pattern, replacement [, start [, N ]] [, flags ])
fn regexp_replace(
    text: &str,
    start: i32,
    n: Option<i32>, // `None` if not specified
    ctx: &RegexpContext,
) -> Result<Box<str>> {
    // The start position to begin the search
    let start = match start {
        ..=0 => {
            return Err(ExprError::InvalidParam {
                name: "start",
                reason: start.to_string().into(),
            })
        }
        _ => start as usize - 1,
    };

    // This is because the source text may contain unicode
    let start = match text.char_indices().nth(start) {
        Some((idx, _)) => idx,
        // With no match
        None => return Ok(text.into()),
    };

    if n.is_none() && ctx.global || n == Some(0) {
        // --------------------------------------------------------------
        // `-g` enabled (& `N` is not specified) or `N` is `0`          |
        // We need to replace all the occurrence of the matched pattern |
        // --------------------------------------------------------------

        // See if there is capture group or not
        if ctx.regex.captures_len() <= 1 {
            // There is no capture groups in the regex
            // Just replace all matched patterns after `start`
            Ok(format!(
                "{}{}",
                &text[..start],
                ctx.regex.replace_all(&text[start..], &ctx.replacement)
            )
            .into())
        } else {
            // The position to start searching for replacement
            let mut search_start = start;

            // Construct the return string
            let mut ret = text[..search_start].to_string();

            // Begin the actual replace logic
            while let Ok(Some(capture)) = ctx.regex.captures(&text[search_start..]) {
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
                capture.expand(&ctx.replacement, &mut ret);

                // Update the `search_start`
                search_start += match_end;
            }

            // Push the rest of the text to return string
            ret.push_str(&text[search_start..]);

            Ok(ret.into())
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
            if let Some(n) = n {
                // Replace only the N-th match
                let mut count = 1;
                // The absolute index for the start of searching
                let mut search_start = start;
                while let Ok(Some(capture)) = ctx.regex.captures(&text[search_start..]) {
                    // Get the current start & end index
                    let match_start = capture.get(0).unwrap().start();
                    let match_end = capture.get(0).unwrap().end();

                    if count == n {
                        // We've reached the pattern to replace
                        // Let's construct the return string
                        ret = format!(
                            "{}{}{}",
                            &text[..search_start + match_start],
                            &ctx.replacement,
                            &text[search_start + match_end..]
                        );
                        break;
                    }

                    // Update the counter
                    count += 1;

                    // Update `start`
                    search_start += match_end;
                }
            } else {
                // `N` is not specified
                ret.push_str(&ctx.regex.replacen(&text[start..], 1, &ctx.replacement));
            }
        } else {
            // There are capture groups in the regex
            // Reset return string at the beginning
            ret = "".to_string();
            if let Some(n) = n {
                // Replace only the N-th match
                let mut count = 1;
                while let Ok(Some(capture)) = ctx.regex.captures(&text[start..]) {
                    if count == n {
                        // We've reached the pattern to replace
                        let match_start = capture.get(0).unwrap().start();
                        let match_end = capture.get(0).unwrap().end();

                        // Get the replaced string and expand it
                        capture.expand(&ctx.replacement, &mut ret);

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
            } else {
                // `N` is not specified
                if let Ok(None) = ctx.regex.captures(&text[start..]) {
                    // No match
                    return Ok(text.into());
                }

                // Otherwise replace the source text
                if let Ok(Some(capture)) = ctx.regex.captures(&text[start..]) {
                    let match_start = capture.get(0).unwrap().start();
                    let match_end = capture.get(0).unwrap().end();

                    // Get the replaced string and expand it
                    capture.expand(&ctx.replacement, &mut ret);

                    // Construct the return string
                    ret = format!(
                        "{}{}{}",
                        &text[..start + match_start],
                        ret,
                        &text[start + match_end..]
                    );
                }
            }
        }

        Ok(ret.into())
    }
}

#[function(
    // regexp_split_to_array(source, pattern)
    "regexp_split_to_array(varchar, varchar) -> varchar[]",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    // regexp_split_to_array(source, pattern, flags)
    "regexp_split_to_array(varchar, varchar, varchar) -> varchar[]",
    prebuild = "RegexpContext::from_pattern_flags($1, $2)?"
)]
fn regexp_split_to_array(text: &str, regex: &RegexpContext) -> Option<ListValue> {
    let n = text.len();
    let mut start = 0;
    let mut builder = Utf8ArrayBuilder::new(0);
    let mut empty_flag = false;

    loop {
        if start >= n {
            // Prevent overflow
            break;
        }

        let capture = regex.regex.captures(&text[start..]).unwrap();

        if capture.is_none() {
            break;
        }

        let whole_match = capture.unwrap().get(0);
        debug_assert!(whole_match.is_some(), "Expected `whole_match` to be valid");

        let begin = whole_match.unwrap().start() + start;
        let end = whole_match.unwrap().end() + start;

        if begin == end {
            // Empty match (i.e., `\s*`)
            empty_flag = true;

            if begin == text.len() {
                // We do not need to push extra stuff to the result list
                start = begin;
                break;
            }
            builder.append(Some(&text[start..begin + 1]));
            start = end + 1;
            continue;
        }

        if start == begin {
            // The before match is possibly empty
            if !empty_flag {
                // We'll push an empty string to conform with postgres
                // If there does not exists a empty match before
                builder.append(Some(""));
            }
            start = end;
            continue;
        }

        if begin != 0 {
            // Normal case
            builder.append(Some(&text[start..begin]));
        }

        // We should update the `start` no matter `begin` is zero or not
        start = end;
    }

    if start < n {
        // Push the extra text to the list
        // Note that this will implicitly push the entire text to the list
        // If there is no match, which is the expected behavior
        builder.append(Some(&text[start..]));
    }

    if start == n && !empty_flag {
        builder.append(Some(""));
    }

    Some(ListValue::new(builder.finish().into()))
}
