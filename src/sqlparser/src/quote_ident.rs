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

use std::fmt::{self, Display};

use crate::keywords;

/// A wrapper that returns the given string suitably quoted to be used as an identifier in an SQL
/// statement string in its `Display` implementation.
/// Quotes are added only if necessary (i.e., if the string contains non-identifier characters,
/// would be case-folded, or is a SQL keyword). Embedded quotes are properly doubled.
///
/// Refer to <https://github.com/postgres/postgres/blob/90189eefc1e11822794e3386d9bafafd3ba3a6e8/src/backend/utils/adt/ruleutils.c#L11506>
pub struct QuoteIdent<'a>(pub &'a str);

impl QuoteIdent<'_> {
    pub(crate) fn needs_quotes(ident: &str) -> bool {
        !is_simple_identifier(ident) || is_keyword(ident)
    }
}

impl Display for QuoteIdent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !Self::needs_quotes(self.0) {
            return f.write_str(self.0);
        }

        f.write_str("\"")?;
        for c in self.0.chars() {
            if c == '"' {
                f.write_str("\"\"")?;
            } else {
                write!(f, "{c}")?;
            }
        }
        f.write_str("\"")
    }
}

fn is_simple_identifier(ident: &str) -> bool {
    let mut chars = ident.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !matches!(first, 'a'..='z' | '_') {
        return false;
    }

    for ch in chars {
        if !matches!(ch, 'a'..='z' | '0'..='9' | '_') {
            return false;
        }
    }

    true
}

fn is_keyword(ident: &str) -> bool {
    let ident_upper = ident.to_ascii_uppercase();
    // NOTE: PostgreSQL only quotes non-UNRESERVED keywords. We stay conservative here
    // and quote any token that matches our keyword list to avoid ambiguity.
    keywords::ALL_KEYWORDS
        .binary_search(&ident_upper.as_str())
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::QuoteIdent;

    fn check(input: &str, expect: &str) {
        assert_eq!(QuoteIdent(input).to_string(), expect);
    }

    #[test]
    fn quote_ident_basic() {
        check("foo", "foo");
        check("foo_bar", "foo_bar");
        check("foo1", "foo1");
        check("1foo", "\"1foo\"");
        check("foo-bar", "\"foo-bar\"");
        check("FooBar", "\"FooBar\"");
    }

    #[test]
    fn quote_ident_keyword() {
        check("select", "\"select\"");
        check("with", "\"with\"");
    }

    #[test]
    fn quote_ident_embedded_quote() {
        check("foo\"bar", "\"foo\"\"bar\"");
    }
}
