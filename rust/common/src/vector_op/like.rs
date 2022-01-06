use crate::error::Result;

#[inline(always)]
pub fn like_default(s: &str, p: &str) -> Result<bool> {
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
        return Ok(false);
    }
    Ok(true)
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
            let output = like_default(target, pattern).unwrap();
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
