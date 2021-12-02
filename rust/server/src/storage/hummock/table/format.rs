use bytes::BufMut;
use std::{ptr, u64};

/// Append `u64::MAX - timestamp` to user key
///
/// In this way, the keys can be comparable even with the timestamp.
/// And a key with larger timestamp will be smaller.
pub fn key_with_ts(mut key: Vec<u8>, ts: u64) -> Vec<u8> {
    let res = (u64::MAX - ts).to_be();
    key.reserve(8);
    let buf = key.chunk_mut();
    unsafe {
        ptr::copy_nonoverlapping(
            &res as *const u64 as *const u8,
            buf.as_mut_ptr() as *mut _,
            8,
        );
        key.advance_mut(8);
    }

    key
}

/// Extract timestamp part from key
pub fn get_ts(key: &[u8]) -> u64 {
    let mut ts: u64 = 0;
    unsafe {
        let src = &key[key.len() - 8..];
        ptr::copy_nonoverlapping(src.as_ptr(), &mut ts as *mut u64 as *mut u8, 8);
    }
    u64::MAX - u64::from_be(ts)
}

/// Extract user key without timestamp part
pub fn user_key(key: &[u8]) -> &[u8] {
    &key[..key.len() - 8]
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn test_key_ts() {
        let key = key_with_ts("aaa".to_string().encode_to_vec(), 233);
        assert_eq!(get_ts(&key), 233);
    }
}
