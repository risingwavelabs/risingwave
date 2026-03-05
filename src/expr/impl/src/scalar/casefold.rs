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

use risingwave_expr::function;
use unicode_casefold::UnicodeCaseFold;

/// Unicode case folding for case-insensitive comparison.
///
/// Unlike `lower()`, `casefold()` handles multi-character expansions
/// (e.g., 'ß' → "ss") and normalizes all case variants to a single form
/// (e.g., 'Σ', 'σ', 'ς' all fold to 'σ').
///
/// ```slt
/// query T
/// SELECT casefold('Hello World');
/// ----
/// hello world
///
/// query T
/// SELECT casefold('Straße');
/// ----
/// strasse
///
/// query T
/// SELECT casefold(NULL::varchar);
/// ----
/// NULL
/// ```
#[function("casefold(varchar) -> varchar")]
fn casefold(s: &str, writer: &mut impl std::fmt::Write) {
    for c in s.case_fold() {
        writer.write_char(c).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_casefold() {
        let cases = [
            ("Hello World", "hello world"),
            ("HELLO RUST", "hello rust"),
            // ß folds to ss
            ("Straße", "strasse"),
            // Final sigma (ς) and capital sigma (Σ) both fold to σ
            ("ΣΊΣΥΦΟΣ", "σίσυφοσ"),
            ("", ""),
        ];

        for (input, expected) in cases {
            let mut writer = String::new();
            casefold(input, &mut writer);
            assert_eq!(writer, expected, "casefold({:?})", input);
        }
    }
}
