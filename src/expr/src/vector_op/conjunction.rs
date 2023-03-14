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

use risingwave_expr_macro::function;

// see BinaryShortCircuitExpression
// #[function("and(boolean, boolean) -> boolean")]
pub fn and(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(lb), Some(lr)) => Some(lb & lr),
        (Some(true), None) => None,
        (None, Some(true)) => None,
        (Some(false), None) => Some(false),
        (None, Some(false)) => Some(false),
        (None, None) => None,
    }
}

// see BinaryShortCircuitExpression
// #[function("or(boolean, boolean) -> boolean")]
pub fn or(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(lb), Some(lr)) => Some(lb | lr),
        (Some(true), None) => Some(true),
        (None, Some(true)) => Some(true),
        (Some(false), None) => None,
        (None, Some(false)) => None,
        (None, None) => None,
    }
}

#[function("not(boolean) -> boolean")]
pub fn not(v: bool) -> bool {
    !v
}

#[cfg(test)]
mod tests {
    use crate::vector_op::conjunction::{and, or};

    #[test]
    fn test_and() {
        assert_eq!(Some(true), and(Some(true), Some(true)));
        assert_eq!(Some(false), and(Some(true), Some(false)));
        assert_eq!(Some(false), and(Some(false), Some(false)));
        assert_eq!(None, and(Some(true), None));
        assert_eq!(Some(false), and(Some(false), None));
        assert_eq!(None, and(None, None));
    }

    #[test]
    fn test_or() {
        assert_eq!(Some(true), or(Some(true), Some(true)));
        assert_eq!(Some(true), or(Some(true), Some(false)));
        assert_eq!(Some(false), or(Some(false), Some(false)));
        assert_eq!(Some(true), or(Some(true), None));
        assert_eq!(None, or(Some(false), None));
        assert_eq!(None, or(None, None));
    }
}
