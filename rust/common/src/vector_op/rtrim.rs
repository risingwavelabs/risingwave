use crate::array::{BytesGuard, BytesWriter};
use crate::error::Result;

/// Note: the behavior of `rtrim` in `PostgreSQL` and `trim_end` (or `trim_right`) in Rust
/// are actually different when the string is in right-to-left languages like Arabic or Hebrew.
/// Since we would like to simplify the implementation, currently we omit this case.
#[inline(always)]
pub fn rtrim(s: &str, writer: BytesWriter) -> Result<BytesGuard> {
    writer.write_ref(s.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayBuilder, UTF8ArrayBuilder};

    #[test]
    fn test_rtrim() -> Result<()> {
        let cases = [
            (" \tHello\tworld\t ", " \tHello\tworld"),
            (" \t空I ❤️ databases空\t ", " \t空I ❤️ databases空"),
        ];

        for (s, expected) in cases {
            let builder = UTF8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = rtrim(s, writer)?;
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
