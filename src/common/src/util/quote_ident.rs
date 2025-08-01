// Copyright 2025 RisingWave Labs
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

use std::fmt::Display;

/// A wrapper that returns the given string suitably quoted to be used as an identifier in an SQL
/// statement string in its `Display` implementation.
/// Quotes are added only if necessary (i.e., if the string contains non-identifier characters or
/// would be case-folded). Embedded quotes are properly doubled.
///
/// Refer to <https://github.com/postgres/postgres/blob/90189eefc1e11822794e3386d9bafafd3ba3a6e8/src/backend/utils/adt/ruleutils.c#L11506>
pub struct QuoteIdent<'a>(pub &'a str);

impl Display for QuoteIdent<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let needs_quotes = self
            .0
            .chars()
            .any(|c| !matches!(c, 'a'..='z' | '0'..='9' | '_'));

        if !needs_quotes {
            self.0.fmt(f)?;
        } else {
            write!(f, "\"")?;
            for c in self.0.chars() {
                if c == '"' {
                    write!(f, "\"\"")?;
                } else {
                    write!(f, "{c}")?;
                }
            }
            write!(f, "\"")?;
        }

        Ok(())
    }
}
