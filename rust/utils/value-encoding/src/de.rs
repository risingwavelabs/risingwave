use bytes::Buf;
use memcomparable::Result;
/// A structure that deserializes memcomparable bytes into Rust values.
pub struct Deserializer<B: Buf> {
    inner: memcomparable::Deserializer<B>,
}

impl<B: Buf> Deserializer<B> {
    /// Creates a deserializer from a buffer.
    pub fn new(input: B) -> Self {
        Deserializer {
            inner: memcomparable::Deserializer::new(input),
        }
    }

    /// Set whether data is serialized in reverse order.
    pub fn set_reverse(&mut self, reverse: bool) {
        self.inner.set_reverse(reverse);
    }

    /// Unwrap the inner buffer from the `Deserializer`.
    pub fn into_inner(self) -> B {
        self.inner.into_inner()
    }

    /// The inner is just a memcomparable deserializer. Removed in future.
    pub fn memcom_de(&mut self) -> &mut memcomparable::Deserializer<B> {
        &mut self.inner
    }

    /// Read u8 from Bytes input in decimal form (Do not include null tag). Used by value encoding
    /// ([`serialize_cell`]). TODO: It is a temporal solution For value encoding. Will moved to
    /// value encoding serializer in future.
    pub fn read_decimal_v2(&mut self) -> Result<Vec<u8>> {
        self.inner.read_decimal_v2()
    }
}
