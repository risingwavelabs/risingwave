use bytes::{Buf, BufMut};

use super::coding::CacheKey;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct TestCacheKey(pub u64);

impl CacheKey for TestCacheKey {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
    }

    fn decode(mut buf: &[u8]) -> Self {
        Self(buf.get_u64())
    }
}
