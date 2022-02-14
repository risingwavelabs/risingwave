use crate::array::{BytesGuard, BytesWriter};
use crate::error::Result;

#[inline(always)]
pub fn lower(s: &str, writer: BytesWriter) -> Result<BytesGuard> {
    writer.write_ref(&s.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    #[test]
    fn test_lower() -> Result<()> {
        let cases = [
            ("HELLO WORLD", "hello world"),
            ("hello RUST", "hello rust"),
            ("3 PINEAPPLES", "3 pineapples"),
        ];

        for (s, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = lower(s, writer)?;
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
