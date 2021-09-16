use std::hash::{BuildHasher, Hasher};

pub(crate) fn finalize_hashers<H: Hasher>(hashers: &mut Vec<H>) -> Vec<u64> {
    return hashers
        .iter()
        .map(|hasher| hasher.finish())
        .collect::<Vec<u64>>();
}

pub(crate) struct CRC32FastBuilder;
impl BuildHasher for CRC32FastBuilder {
    type Hasher = crc32fast::Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        crc32fast::Hasher::new()
    }
}
