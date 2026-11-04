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

//! Text analysis shared by the `keyword_match` function and full-text indexes.
//!
//! The behavior of an analyzer is part of the on-disk index format: an index built with one
//! analyzer version can only be searched with exactly the same tokenization. Any behavior change
//! must therefore bump [`Analyzer::version`].

use std::ops::ControlFlow;

use crate::types::{JsonbRef, ScalarRefImpl};

pub const DEFAULT_ANALYZER: &str = "simple";

/// Tokens longer than this many bytes are dropped.
pub const MAX_TOKEN_BYTES: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Analyzer {
    /// Splits on non-alphanumeric characters, lowercases, and drops tokens longer than
    /// [`MAX_TOKEN_BYTES`] bytes.
    Simple,
}

impl Analyzer {
    /// Exact, case-sensitive name lookup ("simple"). Returns None for unknown names.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "simple" => Some(Self::Simple),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Simple => "simple",
        }
    }

    /// Version of the analyzer behavior. Bump when behavior changes. Simple = 1.
    pub fn version(self) -> u32 {
        match self {
            Self::Simple => 1,
        }
    }

    /// Calls `f` with every token of `text`, in order.
    ///
    /// Simple analyzer: split on every char where `!char::is_alphanumeric()`, lowercase each token
    /// with `str::to_lowercase`, skip empty tokens and tokens longer than [`MAX_TOKEN_BYTES`] bytes
    /// (measured after lowercasing).
    pub fn tokenize(self, text: &str, mut f: impl FnMut(&str)) {
        let _ = self.try_tokenize(text, &mut |token| {
            f(token);
            ControlFlow::Continue(())
        });
    }

    /// Like [`Analyzer::tokenize`], but stops as soon as `f` returns `Break`.
    fn try_tokenize(
        self,
        text: &str,
        f: &mut impl FnMut(&str) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        match self {
            Self::Simple => {
                let mut ascii_buf = String::new();
                for piece in text.split(|c: char| !c.is_alphanumeric()) {
                    if piece.is_empty() {
                        continue;
                    }
                    if piece.is_ascii() {
                        // ASCII lowercasing preserves the byte length.
                        if piece.len() > MAX_TOKEN_BYTES {
                            continue;
                        }
                        if piece.bytes().any(|b| b.is_ascii_uppercase()) {
                            ascii_buf.clear();
                            ascii_buf.push_str(piece);
                            ascii_buf.make_ascii_lowercase();
                            f(&ascii_buf)?;
                        } else {
                            f(piece)?;
                        }
                    } else {
                        // Non-ASCII lowercasing can change the byte length (e.g. 'İ'), and
                        // `str::to_lowercase` handles context-sensitive cases such as final sigma.
                        let lower = piece.to_lowercase();
                        if lower.len() > MAX_TOKEN_BYTES {
                            continue;
                        }
                        f(&lower)?;
                    }
                }
                ControlFlow::Continue(())
            }
        }
    }
}

/// Calls `f` with every string value inside a JSONB value (nested objects and arrays, depth-first,
/// document order). Object keys and non-string scalars are ignored.
pub fn for_each_jsonb_string(value: JsonbRef<'_>, mut f: impl FnMut(&str)) {
    let _ = try_for_each_jsonb_string(value, &mut |s| {
        f(s);
        ControlFlow::Continue(())
    });
}

fn try_for_each_jsonb_string(
    value: JsonbRef<'_>,
    f: &mut impl FnMut(&str) -> ControlFlow<()>,
) -> ControlFlow<()> {
    if let Ok(s) = value.as_str() {
        f(s)?;
    } else if value.is_array() {
        for elem in value.array_elements().unwrap() {
            try_for_each_jsonb_string(elem, f)?;
        }
    } else if value.is_object() {
        for (_key, v) in value.object_key_values().unwrap() {
            try_for_each_jsonb_string(v, f)?;
        }
    }
    ControlFlow::Continue(())
}

/// Tokenizes a document datum. Supports `ScalarRefImpl::Utf8` and `ScalarRefImpl::Jsonb` (all
/// string values). Panics on other types.
pub fn tokenize_document(analyzer: Analyzer, doc: ScalarRefImpl<'_>, mut f: impl FnMut(&str)) {
    let _ = try_tokenize_document(analyzer, doc, &mut |token| {
        f(token);
        ControlFlow::Continue(())
    });
}

fn try_tokenize_document(
    analyzer: Analyzer,
    doc: ScalarRefImpl<'_>,
    f: &mut impl FnMut(&str) -> ControlFlow<()>,
) -> ControlFlow<()> {
    match doc {
        ScalarRefImpl::Utf8(text) => analyzer.try_tokenize(text, f),
        ScalarRefImpl::Jsonb(jsonb) => {
            try_for_each_jsonb_string(jsonb, &mut |s| analyzer.try_tokenize(s, f))
        }
        other => panic!(
            "full-text document must be varchar or jsonb, got {}",
            other.get_ident()
        ),
    }
}

/// Analyzes a keyword query into sorted, de-duplicated tokens. May return an empty Vec.
pub fn analyze_query(analyzer: Analyzer, query: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    analyzer.tokenize(query, |token| tokens.push(token.to_owned()));
    tokens.sort_unstable();
    tokens.dedup();
    tokens
}

/// Whether the document contains every token in `query_tokens` (which must come from
/// `analyze_query`).
pub fn document_matches(
    analyzer: Analyzer,
    doc: ScalarRefImpl<'_>,
    query_tokens: &[String],
) -> bool {
    debug_assert!(
        query_tokens.is_sorted() && query_tokens.windows(2).all(|w| w[0] != w[1]),
        "query tokens must be sorted and de-duplicated"
    );
    if query_tokens.is_empty() {
        return true;
    }
    let mut found = vec![false; query_tokens.len()];
    let mut remaining = query_tokens.len();
    let _ = try_tokenize_document(analyzer, doc, &mut |token| {
        if let Ok(idx) = query_tokens.binary_search_by(|t| t.as_str().cmp(token))
            && !found[idx]
        {
            found[idx] = true;
            remaining -= 1;
            if remaining == 0 {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    remaining == 0
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::types::{JsonbVal, Scalar};

    fn tokens(text: &str) -> Vec<String> {
        let mut out = Vec::new();
        Analyzer::Simple.tokenize(text, |t| out.push(t.to_owned()));
        out
    }

    fn jsonb_strings(json: &str) -> Vec<String> {
        let value = JsonbVal::from_str(json).unwrap();
        let mut out = Vec::new();
        for_each_jsonb_string(value.as_scalar_ref(), |s| out.push(s.to_owned()));
        out
    }

    fn jsonb_tokens(json: &str) -> Vec<String> {
        let value = JsonbVal::from_str(json).unwrap();
        let mut out = Vec::new();
        tokenize_document(
            Analyzer::Simple,
            ScalarRefImpl::Jsonb(value.as_scalar_ref()),
            |t| out.push(t.to_owned()),
        );
        out
    }

    fn text_matches(doc: &str, query: &str) -> bool {
        let query_tokens = analyze_query(Analyzer::Simple, query);
        document_matches(Analyzer::Simple, ScalarRefImpl::Utf8(doc), &query_tokens)
    }

    #[test]
    fn test_analyzer_names() {
        assert_eq!(Analyzer::from_name("simple"), Some(Analyzer::Simple));
        assert_eq!(
            Analyzer::from_name(DEFAULT_ANALYZER),
            Some(Analyzer::Simple)
        );
        assert_eq!(Analyzer::from_name("Simple"), None);
        assert_eq!(Analyzer::from_name("english"), None);
        assert_eq!(Analyzer::from_name(""), None);
        assert_eq!(Analyzer::Simple.name(), "simple");
        assert_eq!(Analyzer::Simple.version(), 1);
    }

    #[test]
    fn test_tokenize_ascii() {
        assert_eq!(tokens("hello world"), ["hello", "world"]);
        assert_eq!(tokens(""), Vec::<String>::new());
        assert_eq!(tokens("   "), Vec::<String>::new());
    }

    #[test]
    fn test_tokenize_punctuation() {
        assert_eq!(
            tokens("error: db.conn-timeout (code=42)!"),
            ["error", "db", "conn", "timeout", "code", "42"]
        );
        assert_eq!(
            tokens("snake_case/path\\to\tx\ny"),
            ["snake", "case", "path", "to", "x", "y"]
        );
        assert_eq!(tokens("--..!!"), Vec::<String>::new());
    }

    #[test]
    fn test_tokenize_uppercase() {
        assert_eq!(
            tokens("Database CONNECTION TimeOut"),
            ["database", "connection", "timeout"]
        );
    }

    #[test]
    fn test_tokenize_numbers() {
        assert_eq!(tokens("HTTP 503 v2 1.5"), ["http", "503", "v2", "1", "5"]);
    }

    #[test]
    fn test_tokenize_unicode_letters() {
        assert_eq!(tokens("Café Ÿes naïve"), ["café", "ÿes", "naïve"]);
        assert_eq!(tokens("ΣΟΦΙΑ, Straße"), ["σοφια", "straße"]);
        // Lowercasing follows `str::to_lowercase`, including the final sigma rule.
        assert_eq!(tokens("ΟΔΟΣ"), ["οδος"]);
    }

    #[test]
    fn test_tokenize_cjk_single_token_per_run() {
        // Known limitation: CJK ideographs are alphanumeric, so a run of CJK text without
        // separators forms a single token and can only be matched as a whole.
        assert_eq!(tokens("数据库连接超时"), ["数据库连接超时"]);
        assert_eq!(tokens("数据库 连接，超时"), ["数据库", "连接", "超时"]);
        assert!(!text_matches("数据库连接超时", "超时"));
        assert!(text_matches("数据库 连接 超时", "超时"));
    }

    #[test]
    fn test_tokenize_long_token_drop() {
        let max = "a".repeat(MAX_TOKEN_BYTES);
        let too_long = "b".repeat(MAX_TOKEN_BYTES + 1);
        assert_eq!(
            tokens(&format!("x {max} {too_long} y")),
            ["x", max.as_str(), "y"]
        );
        // The limit applies to the lowercased token, in bytes.
        let upper = "A".repeat(MAX_TOKEN_BYTES);
        assert_eq!(tokens(&upper), [max.as_str()]);
        // 'é' is 2 bytes: 20 of them fit, 21 do not.
        let fits = "é".repeat(MAX_TOKEN_BYTES / 2);
        let over = "é".repeat(MAX_TOKEN_BYTES / 2 + 1);
        assert_eq!(tokens(&format!("{fits} {over}")), [fits.as_str()]);
        // 'İ' (2 bytes) lowercases to "i̇" (3 bytes), so 14 of them exceed 40 bytes after
        // lowercasing even though the original is only 28 bytes.
        let dotted = "İ".repeat(14);
        assert!(dotted.len() <= MAX_TOKEN_BYTES);
        assert_eq!(tokens(&dotted), Vec::<String>::new());
    }

    #[test]
    fn test_jsonb_strings() {
        assert_eq!(jsonb_strings(r#""just a string""#), ["just a string"]);
        assert_eq!(
            jsonb_strings(r#"[1, true, null, 2.5, "a", ["b", {"k": "c"}]]"#),
            ["a", "b", "c"]
        );
        assert_eq!(
            jsonb_strings(
                r#"{"error": {"message": "Database timeout", "code": 504},
                    "spans": [{"name": "query", "ok": false}, "retry"],
                    "keyonly": null}"#
            ),
            ["Database timeout", "query", "retry"]
        );
        assert_eq!(jsonb_strings("123"), Vec::<String>::new());
        assert_eq!(jsonb_strings("{}"), Vec::<String>::new());
    }

    #[test]
    fn test_jsonb_tokens_ignore_keys_and_numbers() {
        assert_eq!(
            jsonb_tokens(r#"{"Message": "Database TIMEOUT", "code": 504, "tags": ["a-b", 7]}"#),
            ["database", "timeout", "a", "b"]
        );
        let value =
            JsonbVal::from_str(r#"{"message": "connection reset", "status": 503}"#).unwrap();
        let doc = ScalarRefImpl::Jsonb(value.as_scalar_ref());
        let q = |query| analyze_query(Analyzer::Simple, query);
        assert!(document_matches(
            Analyzer::Simple,
            doc,
            &q("reset connection")
        ));
        assert!(!document_matches(Analyzer::Simple, doc, &q("message")));
        assert!(!document_matches(Analyzer::Simple, doc, &q("503")));
        assert!(!document_matches(Analyzer::Simple, doc, &q("status reset")));
    }

    #[test]
    fn test_analyze_query() {
        assert_eq!(
            analyze_query(Analyzer::Simple, "Timeout database TIMEOUT, db"),
            ["database", "db", "timeout"]
        );
        assert!(analyze_query(Analyzer::Simple, " ,.;! ").is_empty());
        assert!(analyze_query(Analyzer::Simple, "").is_empty());
    }

    #[test]
    fn test_document_matches_and_semantics() {
        assert!(text_matches(
            "Database connection timeout",
            "database timeout"
        ));
        assert!(text_matches(
            "Request timeout while connecting to database",
            "database timeout"
        ));
        assert!(!text_matches(
            "Database query completed",
            "database timeout"
        ));
        assert!(!text_matches("databases timed out", "database timeout"));
        // Token match, not substring match.
        assert!(!text_matches("runtime error", "timeout"));
        assert!(!text_matches("runtime error", "time"));
        // Duplicated query tokens do not require duplicated document tokens.
        assert!(text_matches("timeout", "timeout TIMEOUT timeout"));
        // An empty query token list trivially matches.
        assert!(document_matches(
            Analyzer::Simple,
            ScalarRefImpl::Utf8("x"),
            &[]
        ));
        assert!(!text_matches("", "x"));
    }

    #[test]
    #[should_panic]
    fn test_tokenize_document_unsupported_type() {
        tokenize_document(Analyzer::Simple, ScalarRefImpl::Int32(1), |_| {});
    }
}
