use crate::array2::{Array, ArrayBuilder, PrimitiveArray, UTF8Array, UTF8ArrayBuilder};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use std::cmp::min;

pub fn vector_substr_start(utf8_array: &UTF8Array, off: &PrimitiveArray<i32>) -> Result<UTF8Array> {
    let mut builder = UTF8ArrayBuilder::new(utf8_array.len())?;
    for (item, off) in utf8_array.iter().zip(off.iter()) {
        match (item, off) {
            (Some(s), Some(off)) => {
                let start = min(off as usize, s.len());
                builder.append(Some(&s[(start as usize)..]))
            }?,
            (None, _) => builder.append(None)?,
            _ => {
                return Err(
                    InternalError("off of substr function can not be None".to_string()).into(),
                )
            }
        };
    }
    builder.finish()
}

pub fn vector_substr_end(utf8_array: &UTF8Array, len: &PrimitiveArray<i32>) -> Result<UTF8Array> {
    let mut builder = UTF8ArrayBuilder::new(utf8_array.len())?;
    for (item, len) in utf8_array.iter().zip(len.iter()) {
        match (item, len) {
            (Some(s), Some(len)) => {
                let end = min(len as usize, s.len());
                builder.append(Some(&s[..end]))?
            }
            (None, _) => builder.append(None)?,
            _ => {
                return Err(
                    InternalError("len of substr function can not be None".to_string()).into(),
                )
            }
        };
    }
    builder.finish()
}

pub fn vector_substr_start_end(
    utf8_array: &UTF8Array,
    off: &PrimitiveArray<i32>,
    len: &PrimitiveArray<i32>,
) -> Result<UTF8Array> {
    let mut builder = UTF8ArrayBuilder::new(utf8_array.len())?;
    for ((off, len), item) in off.iter().zip(len.iter()).zip(utf8_array.iter()) {
        match (item, off, len) {
            (Some(s), Some(off), Some(len)) => {
                let end = min((off + len) as usize, s.len());
                builder.append(Some(&s[(off as usize)..end]))?
            }
            (None, _, _) => builder.append(None)?,
            _ => {
                return Err(InternalError(
                    "len and off of substr function can not be None".to_string(),
                )
                .into())
            }
        };
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test() {
        test_assert(Some(10), None);
        test_assert(None, Some(10));
        test_assert(Some(10), Some(10));
    }
    fn test_assert(off: Option<i32>, len: Option<i32>) {
        let s = "abcdefghijklmnopqrstuvwxyz";
        let mut start = 0usize;
        let mut end = s.len();
        if let Some(off) = off {
            start = off as usize;
        }
        if let Some(len) = len {
            end = start + (len as usize);
        }

        let mut utf8_vec = Vec::new();
        let mut targets = Vec::new();
        let mut off_vec = Vec::new();
        let mut len_vec = Vec::new();
        for i in 0..100 {
            if i % 2 == 0 {
                utf8_vec.push(Some(s));
                targets.push(Some(&s[start..end]));
            } else {
                utf8_vec.push(None);
                targets.push(None);
            }
            off_vec.push(Some(start as i32));
            len_vec.push(Some((end - start) as i32));
        }

        let utf8_array = UTF8Array::from_slice(utf8_vec.as_ref()).unwrap();
        let off_array = PrimitiveArray::from_slice(off_vec.as_ref()).unwrap();
        let len_array = PrimitiveArray::from_slice(len_vec.as_ref()).unwrap();
        let res = match (off, len) {
            (None, Some(_)) => vector_substr_end(&utf8_array, &len_array).unwrap(),
            (Some(_), None) => vector_substr_start(&utf8_array, &off_array).unwrap(),
            (Some(_), Some(_)) => {
                vector_substr_start_end(&utf8_array, &off_array, &len_array).unwrap()
            }
            (None, None) => panic!("no test"),
        };
        for (x, y) in res.iter().zip(targets.iter()) {
            assert_eq!(x, *y);
        }
    }
}
