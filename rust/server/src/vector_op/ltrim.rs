use crate::array::{BytesGuard, BytesWriter};
use crate::error::Result;

/// Note: the behavior of `ltrim` in `PostgreSQL` and `trim_start` (or `trim_left`) in Rust
/// are actually different when the string is in right-to-left languages like Arabic or Hebrew.
/// Since we would like to simplify the implementation, currently we omit this case.
#[inline(always)]
pub fn ltrim(s: &str, writer: BytesWriter) -> Result<BytesGuard> {
    writer.write_ref(s.trim_start())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayBuilder, UTF8ArrayBuilder};

    #[test]
    fn test_ltrim() -> Result<()> {
        let cases = [
            (" \tHello\tworld\t", "Hello\tworld\t"),
            (" \t空I ❤️ databases空 ", "空I ❤️ databases空 "),
        ];

        for (s, expected) in cases {
            let builder = UTF8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = ltrim(s, writer)?;
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
