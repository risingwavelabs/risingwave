use crate::error::Result;

#[inline(always)]
pub fn ascii(s: &str) -> Result<i32> {
    Ok(s.as_bytes().get(0).map(|x| *x as i32).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_ascii() {
        let cases = [("hello", 104), ("你好", 228), ("", 0)];
        for (s, expected) in cases {
            assert_eq!(ascii(s), Ok(expected))
        }
    }
}
