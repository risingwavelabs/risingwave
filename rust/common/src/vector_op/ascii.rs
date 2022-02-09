use crate::error::Result;

#[inline(always)]
pub fn ascii(s: &str) -> Result<i32> {
    if s.len() == 0 {
        return Ok(0);
    }
    Ok(s.bytes().nth(0).unwrap() as i32)
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
