use crate::array::{BytesGuard, BytesWriter};
use crate::error::Result;

#[inline(always)]
pub fn trim(s: &str, writer: BytesWriter) -> Result<BytesGuard> {
    writer.write_ref(s.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    #[test]
    fn test_trim() -> Result<()> {
        let cases = [
            (" Hello\tworld\t", "Hello\tworld"),
            (" 空I ❤️ databases空 ", "空I ❤️ databases空"),
        ];

        for (s, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = trim(s, writer)?;
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
