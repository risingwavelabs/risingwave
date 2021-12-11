use std::{ptr, u64};

use bytes::BufMut;

/// Convert user key to full key by appending `u64::MAX - timestamp` to the user key.
///
/// In this way, the keys can be comparable even with the timestamp, and a key with a larger
/// timestamp will be smaller and thus be sorted to a upper position.
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
pub fn user_key(full_key: &[u8]) -> &[u8] {
    &full_key[..full_key.len() - 8]
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn test_key_ts() {
        let full_key = key_with_ts("aaa".to_string().encode_to_vec(), 233);
        assert_eq!(get_ts(&full_key), 233);
    }
}
