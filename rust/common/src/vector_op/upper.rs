use crate::array::{BytesGuard, BytesWriter};
use crate::error::Result;

#[inline(always)]
pub fn upper(s: &str, writer: BytesWriter) -> Result<BytesGuard> {
    writer.write_ref(&s.to_uppercase())
}

#[cfg(test)]
mod tests {
    use crate::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_upper() -> Result<()> {
        let cases = [
            ("hello world", "HELLO WORLD"),
            ("hello RUST", "HELLO RUST"),
            ("3 apples", "3 APPLES"),
        ];

        for (s, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = upper(s, writer)?;
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
