use bytes::BufMut;

/// A structure for serializing Rust values into a memcomparable bytes.
pub struct Serializer<B: BufMut> {
    inner: memcomparable::Serializer<B>,
}

impl<B: BufMut> Serializer<B> {
    /// Create a new `Serializer`.
    pub fn new(buffer: B) -> Self {
        Serializer {
            inner: memcomparable::Serializer::new(buffer),
        }
    }

    /// Unwrap the inner buffer from the `Serializer`.
    pub fn into_inner(self) -> B {
        self.inner.into_inner()
    }

    /// Set whether data is serialized in reverse order.
    pub fn set_reverse(&mut self, reverse: bool) {
        self.inner.set_reverse(reverse)
    }

    /// The inner is just a memcomparable serializer. Removed in future.
    pub fn memcom_ser(&mut self) -> &mut memcomparable::Serializer<B> {
        &mut self.inner
    }
}
