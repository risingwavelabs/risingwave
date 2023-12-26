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

fn like_impl<const ESCAPE: u8, const CASE_INSENSITIVE: bool>(s: &str, p: &str) -> bool {
    let (mut px, mut sx) = (0, 0);
    let (mut next_px, mut next_sx) = (0, 0);
    let (pbytes, sbytes) = (p.as_bytes(), s.as_bytes());
    while px < pbytes.len() || sx < sbytes.len() {
        if px < pbytes.len() {
            let c = pbytes[px];
            match c {
                b'_' => {
                    if sx < sbytes.len() {
                        px += 1;
                        sx += 1;
                        continue;
                    }
                }
                b'%' => {
                    next_px = px;
                    next_sx = sx + 1;
                    px += 1;
                    continue;
                }
                mut pc => {
                    if ((!CASE_INSENSITIVE && pc == ESCAPE)
                        || (CASE_INSENSITIVE && pc.eq_ignore_ascii_case(&ESCAPE)))
                        && px + 1 < pbytes.len()
                    {
                        px += 1;
                        pc = pbytes[px];
                    }
                    if sx < sbytes.len()
                        && ((!CASE_INSENSITIVE && sbytes[sx] == pc)
                            || (CASE_INSENSITIVE && sbytes[sx].eq_ignore_ascii_case(&pc)))
                    {
                        px += 1;
                        sx += 1;
                        continue;
                    }
                }
            }
        }
        if 0 < next_sx && next_sx <= sbytes.len() {
            px = next_px;
            sx = next_sx;
            continue;
        }
        return false;
    }
    true
}

#[function("like(varchar, varchar) -> boolean")]
pub fn like_default(s: &str, p: &str) -> bool {
    like_impl::<b'\\', false>(s, p)
}

#[function("i_like(varchar, varchar) -> boolean")]
pub fn i_like_default(s: &str, p: &str) -> bool {
    like_impl::<b'\\', true>(s, p)
}

#[cfg(test)]
mod tests {
    use super::{i_like_default, like_default};

    static CASES: &[(&str, &str, bool, bool)] = &[
        (r#"ABCDE"#, r#"%abcde%"#, false, false),
        (r#"Like, expression"#, r#"Like, expression"#, false, true),
        (r#"Like, expression"#, r#"Like, %"#, false, true),
        (r#"Like, expression"#, r#"%, expression"#, false, true),
        (r#"like"#, r#"li%ke"#, false, true),
        (r#"like"#, r#"l%ik%e"#, false, true),
        (r#"like"#, r#"%like%"#, false, true),
        (r#"like"#, r#"l%i%k%e%"#, false, true),
        (r#"like"#, r#"_%_e"#, false, true),
        (r#"like"#, r#"l%__"#, false, true),
        (r#"like"#, r#"_%_%_%_"#, false, true),
        (r#"abctest"#, r#"__test"#, false, false),
        (r#"abctest"#, r#"%_test"#, false, true),
        (r#"aaaaabbb"#, r#"a%a%a%a%a%a%b"#, false, false),
        (
            r#"blush thistle blue yellow saddle"#,
            r#"%yellow%"#,
            false,
            true,
        ),
        (r#"ABC_123"#, r#"ABC_123"#, false, true),
        (r#"ABCD123"#, r#"ABC_123"#, false, true),
        (r#"ABC_123"#, r"ABC\_123", false, true),
        (r#"ABCD123"#, r"ABC\_123", false, false),
        (r"ABC\123", r#"ABC_123"#, false, true),
        (r"ABC\123", r"ABC\\123", false, true),
        (r"ABC\123", r"ABC\123", false, false),
        ("apple", r#"App%"#, true, true),
        ("banana", r#"B%nana"#, true, true),
        ("apple", r#"B%nana"#, true, false),
        ("grape", "Gr_P_", true, true),
    ];

    #[test]
    fn test_like() {
        for (target, pattern, case_insensitive, expected) in CASES {
            let output = if *case_insensitive {
                i_like_default(target, pattern)
            } else {
                like_default(target, pattern)
            };
            assert_eq!(
                output, *expected,
                "target={}, pattern={}, case_insensitive={}",
                target, pattern, case_insensitive
            );
        }
    }
}
