use crate::array::{BytesGuard, BytesWriter};
use crate::error::{ErrorCode, Result};
use std::cmp::{max, min};

#[inline(always)]
pub fn substr_start(s: &str, start: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let start = min(max(start - 1, 0) as usize, s.len());
    writer.write_ref(&s[start..])
}

#[inline(always)]
pub fn substr_for(s: &str, count: i32, writer: BytesWriter) -> Result<BytesGuard> {
    let end = min(count as usize, s.len());
    writer.write_ref(&s[..end])
}

#[inline(always)]
pub fn substr_start_for(
    s: &str,
    start: i32,
    count: i32,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    if count < 0 {
        return Err(ErrorCode::InvalidInputSyntax(
            String::from("non-negative substring length"),
            count.to_string(),
        )
        .into());
    }
    let begin = max(start - 1, 0) as usize;
    let end = min(max(start - 1 + count, 0) as usize, s.len());
    writer.write_ref(&s[begin..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    #[test]
    fn test_substr() -> Result<()> {
        let s = "cxscgccdd";

        let cases = [
            (s.to_owned(), Some(4), None, "cgccdd"),
            (s.to_owned(), None, Some(3), "cxs"),
            (s.to_owned(), Some(4), Some(-2), "[unused result]"),
            (s.to_owned(), Some(4), Some(2), "cg"),
            (s.to_owned(), Some(-1), Some(-5), "[unused result]"),
            (s.to_owned(), Some(-1), Some(5), "cxs"),
        ];

        for (s, off, len, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1)?;
            let writer = builder.writer();
            let guard = match (off, len) {
                (Some(off), Some(len)) => {
                    let result = substr_start_for(&s, off, len, writer);
                    if len < 0 {
                        assert!(result.is_err());
                        continue;
                    } else {
                        result?
                    }
                }
                (Some(off), None) => substr_start(&s, off, writer)?,
                (None, Some(len)) => substr_for(&s, len, writer)?,
                _ => unreachable!(),
            };
            let array = guard.into_inner().finish()?;
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
        Ok(())
    }
}
