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

#[function("like(varchar, varchar) -> boolean")]
pub fn like_default(s: &str, p: &str) -> bool {
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
                pc => {
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

#[cfg(test)]
mod tests {
    use super::like_default;

    static CASES: &[(&str, &str, std::option::Option<bool>)] = &[
        (r#"ABCDE"#, r#"%abcde%"#, Some(false)),
        (r#"Like, expression"#, r#"Like, expression"#, Some(true)),
        (r#"Like, expression"#, r#"Like, %"#, Some(true)),
        (r#"Like, expression"#, r#"%, expression"#, Some(true)),
        (r#"like"#, r#"li%ke"#, Some(true)),
        (r#"like"#, r#"l%ik%e"#, Some(true)),
        (r#"like"#, r#"%like%"#, Some(true)),
        (r#"like"#, r#"l%i%k%e%"#, Some(true)),
        (r#"like"#, r#"_%_e"#, Some(true)),
        (r#"like"#, r#"l%__"#, Some(true)),
        (r#"like"#, r#"_%_%_%_"#, Some(true)),
        (r#"abctest"#, r#"__test"#, Some(false)),
        (r#"abctest"#, r#"%_test"#, Some(true)),
        (r#"aaaaabbb"#, r#"a%a%a%a%a%a%b"#, Some(false)),
        (
            r#"blush thistle blue yellow saddle"#,
            r#"%yellow%"#,
            Some(true),
        ),
    ];

    #[test]
    fn test_like() {
        for (target, pattern, expected) in CASES {
            let output = like_default(target, pattern);
            assert_eq!(
                output,
                expected.unwrap(),
                "target={}, pattern={}",
                target,
                pattern,
            );
        }
    }
}
