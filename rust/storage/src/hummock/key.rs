use super::format::{key_with_ts, user_key};

const VERSION_KEY_SUFFIX_LEN: usize = 8;

/// [`FullKey`] can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// Its format is (`user_key`, `u64::MAX - timestamp`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullKey<T: AsRef<[u8]>>(T);

impl<'a> FullKey<&'a [u8]> {
    pub fn from_slice(full_key: &'a [u8]) -> Self {
        Self(full_key)
    }
}

impl FullKey<Vec<u8>> {
    fn from(full_key: Vec<u8>) -> Self {
        Self(full_key)
    }

    pub fn from_user_key(user_key: Vec<u8>, timestamp: u64) -> Self {
        Self(key_with_ts(user_key, timestamp))
    }

    pub fn from_user_key_slice(user_key: &[u8], timestamp: u64) -> Self {
        Self(key_with_ts(user_key.to_vec(), timestamp))
    }

    pub fn to_user_key(&self) -> &[u8] {
        user_key(self.0.as_slice())
    }
}

impl<T: Eq + AsRef<[u8]>> Ord for FullKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let l_key = &self.0;
        let r_key = &other.0;
        let (l_user_key, l_ver) = l_key
            .as_ref()
            .split_at(l_key.as_ref().len() - VERSION_KEY_SUFFIX_LEN);
        let (r_user_key, r_ver) = r_key
            .as_ref()
            .split_at(r_key.as_ref().len() - VERSION_KEY_SUFFIX_LEN);
        l_user_key.cmp(r_user_key).then_with(|| l_ver.cmp(r_ver))
    }
}

impl<T: Eq + AsRef<[u8]>> PartialOrd for FullKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
