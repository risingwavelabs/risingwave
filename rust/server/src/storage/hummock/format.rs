pub fn user_key(key: &[u8]) -> &[u8] {
    &key[..key.len() - 8]
}
