use crate::error::Result;

#[inline(always)]
/// Location of specified substring
///
/// Note: According to pgsql, position will return 0 rather -1 when substr is not in the target str
pub fn position(str: &str, sub_str: &str) -> Result<i32> {
    match str.find(sub_str) {
        Some(byte_idx) => Ok((str[..byte_idx].chars().count() + 1) as i32),
        None => Ok(0),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_length() {
        let cases = [
            ("hello world", "world", Ok(7)),
            ("床前明月光", "月光", Ok(4)),
            ("床前明月光", "故乡", Ok(0)),
        ];

        for (str, sub_str, expected) in cases {
            println!("position is {}", position(str, sub_str).unwrap());
            assert_eq!(position(str, sub_str), expected)
        }
    }
}
