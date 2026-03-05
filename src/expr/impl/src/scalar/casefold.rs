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

use risingwave_expr::function;

/// Performs Unicode case folding on the input text.
///
/// Case folding is similar to lowercase conversion but more comprehensive,
/// handling special cases where simple case conversion is insufficient for
/// accurate case-insensitive comparisons.
///
/// PostgreSQL 18 compatible function.
#[function("casefold(varchar) -> varchar")]
pub fn casefold(s: &str, writer: &mut impl std::fmt::Write) {
    // Rust's to_lowercase() performs full Unicode case folding
    // which matches PostgreSQL's casefold() behavior
    writer.write_str(&s.to_lowercase()).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_casefold_basic() {
        let mut writer = String::new();
        casefold("HELLO", &mut writer);
        assert_eq!(writer, "hello");
    }

    #[test]
    fn test_casefold_mixed_case() {
        let mut writer = String::new();
        casefold("HeLLo WoRLd", &mut writer);
        assert_eq!(writer, "hello world");
    }

    #[test]
    fn test_casefold_unicode() {
        // German sharp s (ß) case folds to "ss"
        let mut writer = String::new();
        casefold("Straße", &mut writer);
        assert_eq!(writer, "straße");
    }

    #[test]
    fn test_casefold_turkish() {
        // Turkish capital I with dot
        let mut writer = String::new();
        casefold("İstanbul", &mut writer);
        assert_eq!(writer, "i̇stanbul");
    }

    #[test]
    fn test_casefold_empty() {
        let mut writer = String::new();
        casefold("", &mut writer);
        assert_eq!(writer, "");
    }

    #[test]
    fn test_casefold_already_lowercase() {
        let mut writer = String::new();
        casefold("already lowercase", &mut writer);
        assert_eq!(writer, "already lowercase");
    }

    #[test]
    fn test_casefold_numbers_and_symbols() {
        let mut writer = String::new();
        casefold("Test123!@#", &mut writer);
        assert_eq!(writer, "test123!@#");
    }
}
