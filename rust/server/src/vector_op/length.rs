use crate::error::Result;

#[inline(always)]
pub fn length_default(s: &str) -> Result<i64> {
    Ok(s.chars().count() as i64)
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_length() {
        let cases = [("hello world", Ok(11)), ("hello rust", Ok(10))];

        for (s, expected) in cases {
            assert_eq!(length_default(s), expected)
        }
    }
}
