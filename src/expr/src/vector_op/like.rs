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

fn like_impl<const ESCAPE: u8>(s: &str, p: &str) -> bool {
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
                    if pc == ESCAPE && px + 1 < pbytes.len() {
                        px += 1;
                        pc = pbytes[px];
                    }
                    if sx < sbytes.len() && sbytes[sx] == pc {
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
    like_impl::<b'\\'>(s, p)
}

#[cfg(test)]
mod tests {
    use super::like_default;

    static CASES: &[(&str, &str, bool)] = &[
        (r#"ABCDE"#, r#"%abcde%"#, false),
        (r#"Like, expression"#, r#"Like, expression"#, true),
        (r#"Like, expression"#, r#"Like, %"#, true),
        (r#"Like, expression"#, r#"%, expression"#, true),
        (r#"like"#, r#"li%ke"#, true),
        (r#"like"#, r#"l%ik%e"#, true),
        (r#"like"#, r#"%like%"#, true),
        (r#"like"#, r#"l%i%k%e%"#, true),
        (r#"like"#, r#"_%_e"#, true),
        (r#"like"#, r#"l%__"#, true),
        (r#"like"#, r#"_%_%_%_"#, true),
        (r#"abctest"#, r#"__test"#, false),
        (r#"abctest"#, r#"%_test"#, true),
        (r#"aaaaabbb"#, r#"a%a%a%a%a%a%b"#, false),
        (r#"blush thistle blue yellow saddle"#, r#"%yellow%"#, true),
        (r#"ABC_123"#, r#"ABC_123"#, true),
        (r#"ABCD123"#, r#"ABC_123"#, true),
        (r#"ABC_123"#, r#"ABC\_123"#, true),
        (r#"ABCD123"#, r#"ABC\_123"#, false),
        (r#"ABC\123"#, r#"ABC_123"#, true),
        (r#"ABC\123"#, r#"ABC\\123"#, true),
        (r#"ABC\123"#, r#"ABC\123"#, false),
    ];

    #[test]
    fn test_like() {
        for (target, pattern, expected) in CASES {
            let output = like_default(target, pattern);
            assert_eq!(output, *expected, "target={}, pattern={}", target, pattern);
        }
    }
}
