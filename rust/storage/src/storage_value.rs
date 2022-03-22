use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageValue {
    user_value: Bytes,
}

impl From<Bytes> for StorageValue {
    fn from(bytes: Bytes) -> Self {
        Self { user_value: bytes }
    }
}

impl From<Vec<u8>> for StorageValue {
    fn from(data: Vec<u8>) -> Self {
        Self {
            user_value: data.into(),
        }
    }
}

impl StorageValue {
    /// Returns the length of user value (value meta is excluded)
    pub fn len(&self) -> usize {
        self.user_value.len()
    }

    /// Returns whether user value is null
    pub fn is_empty(&self) -> bool {
        self.user_value.is_empty()
    }

    /// Consumes the value and returns a Bytes instance containing identical bytes
    pub fn to_bytes(self) -> Bytes {
        self.user_value
    }

    /// Returns a reference of all Bytes
    pub fn as_bytes(&self) -> &Bytes {
        &self.user_value
    }
}
