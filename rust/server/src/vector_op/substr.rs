use crate::array2::{BytesGuard, BytesWriter};
use crate::error::Result;
use std::cmp::min;

#[inline(always)]
pub fn substr_start(s: &str, off: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let start = min(off as usize, s.len());
    writer.write_ref(&s[start..])
}

#[inline(always)]
pub fn substr_end(s: &str, len: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let end = min(len as usize, s.len());
    writer.write_ref(&s[..end])
}

#[inline(always)]
pub fn substr_start_end(s: &str, off: i32, len: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let end = min((off + len) as usize, s.len());
    writer.write_ref(&s[(off as usize)..end])
}

#[cfg(test)]
mod tests {
    use crate::array2::{Array, ArrayBuilder, UTF8ArrayBuilder};

    use super::*;

    #[test]
    fn test_substr() -> Result<()> {
        let s = "cxscgccdd";

        let cases = [
            (s.to_owned(), Some(3), None, "cgccdd"),
            (s.to_owned(), None, Some(3), "cxs"),
            (s.to_owned(), Some(3), Some(2), "cg"),
        ];

        for (s, off, len, expected) in cases {
            let builder = UTF8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = match (off, len) {
                (Some(off), Some(len)) => substr_start_end(&s, off, len, writer)?,
                (Some(off), None) => substr_start(&s, off, writer)?,
                (None, Some(len)) => substr_end(&s, len, writer)?,
                _ => unreachable!(),
            };
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
