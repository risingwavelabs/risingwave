use crate::array::{BytesGuard, BytesWriter};
use crate::error::Result;

#[inline(always)]
pub fn replace(s: &str, from_str: &str, to_str: &str, writer: BytesWriter) -> Result<BytesGuard> {
    if from_str.is_empty() {
        return writer.write_ref(s);
    }
    let mut last = 0;
    let mut writer = writer.begin();
    while let Some(mut start) = s[last..].find(from_str) {
        start += last;
        writer.write_ref(&s[last..start]).unwrap();
        writer.write_ref(to_str).unwrap();
        last = start + from_str.len();
    }
    writer.write_ref(&s[last..]).unwrap();
    Ok(writer.finish().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    #[test]
    fn test_replace() {
        let cases = vec![
            ("hello, word", "我的", "world", "hello, word"),
            ("hello, word", "", "world", "hello, word"),
            ("hello, word", "word", "world", "hello, world"),
            ("hello, world", "world", "", "hello, "),
            ("你是❤️，是暖，是希望", "是", "非", "你非❤️，非暖，非希望"),
            ("👴笑了", "👴", "爷爷", "爷爷笑了"),
        ];

        for (s, from_str, to_str, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1).unwrap();
            let writer = builder.writer();
            let guard = replace(s, from_str, to_str, writer).unwrap();
            let array = guard.into_inner().finish().unwrap();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }
    }
}
