// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Display, Formatter, Write};
use std::hash::{DefaultHasher, Hasher};
use std::io::Read;
use std::mem;
use std::str::FromStr;

use bytes::{BufMut, Bytes, BytesMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::ArrayType;
use serde::{Deserialize, Serialize};
use to_text::ToText;

/// use uuid::Timestamp;
use crate::array::ArrayResult;
use crate::types::to_binary::ToBinary;
use crate::types::{Buf, DataType, Scalar, ScalarRef, to_text};

/// A UUID data type (128-bit).
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Uuid(pub(crate) uuid::Uuid);

/// A reference to a `Uuid` value.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct UuidRef<'a>(pub &'a uuid::Uuid);

impl Display for UuidRef<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Scalar for Uuid {
    type ScalarRefType<'a> = UuidRef<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        UuidRef(&self.0)
    }
}

impl<'a> ScalarRef<'a> for UuidRef<'a> {
    type ScalarType = Uuid;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        Uuid(*self.0)
    }

    fn hash_scalar<H: Hasher>(&self, state: &mut H) {
        use std::hash::Hash as _;
        self.0.hash(state)
    }
}

impl FromStr for Uuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::from_str(s).map(Self)
    }
}

impl Uuid {
    pub const UUID_SIZE: usize = 16;

    /// Create a new UUID from a 16-byte array
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Creates a UUID using the supplied bytes.
    #[inline]
    pub fn from_u128(v: u128) -> Self {
        Self(uuid::Uuid::from_u128(v))
    }

    /// Return the 128-bit value as a u128.
    #[inline]
    pub fn as_u128(&self) -> u128 {
        self.0.as_u128()
    }

    /// Create a new nil (all zeros) UUID.
    #[inline]
    pub fn nil() -> Self {
        Self(uuid::Uuid::nil())
    }

    /// Generate a random UUID (v4).
    #[inline]
    pub fn new_v4() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Returns the size in bytes of a UUID.
    #[inline]
    pub const fn size() -> usize {
        mem::size_of::<uuid::Uuid>()
    }

    /// Returns the array type for UUIDs in the protocol buffer.
    #[inline]
    pub fn array_type() -> ArrayType {
        ArrayType::Uuid
    }

    /// Creates a UUID from raw little-endian bytes.
    #[inline]
    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes_le(bytes))
    }

    /// Creates a UUID from raw big-endian bytes.
    #[inline]
    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Creates a UUID from uuid string.
    #[inline]
    pub fn from_varchar(s: &str) -> Self {
        match uuid::Uuid::parse_str(s) {
            Ok(uuid) => Self(uuid),
            Err(_) => Self::nil(),
        }
    }

    /// Deserialize from protocol buffer representation.
    pub fn from_protobuf(input: &mut impl Read) -> ArrayResult<Self> {
        let mut buf = [0u8; 16];
        input.read_exact(&mut buf)?;
        Ok(Self::from_be_bytes(buf))
    }

    /// Parse a UUID from binary representation.
    pub fn from_binary(mut input: &[u8]) -> ArrayResult<Self> {
        let mut buf = [0; 16];
        input.read_exact(&mut buf)?;
        Ok(Self::from_be_bytes(buf))
    }

    /// Implementation manually, and is this effective or any other alternate ways to make it more better?
    /// Will keep this implementation here incase of future needs
    #[inline]
    pub fn from_arbitrary_string_builder(s: &str) -> Self {
        // Hash the string to get reproducible bytes
        let mut hasher = DefaultHasher::new();
        std::hash::Hash::hash(&s, &mut hasher);
        let hash = hasher.finish();

        // Extract fields for the UUID
        let d1 = (hash & 0xFFFFFFFF) as u32;
        let d2 = ((hash >> 32) & 0xFFFF) as u16;
        let d3 = ((hash >> 48) & 0xFFFF) as u16;

        // For d4, we'll generate a stable value based on the string length
        let len = s.len() as u64;
        let d4_high = ((len.wrapping_mul(0x9E3779B9) >> 48) & 0xFFFF) as u16;
        let d4_low = ((hash.wrapping_add(len)) & 0xFFFFFFFFFFFF) as u64;

        // Use the builder method with version and variant bits set properly
        let d3_version = (d3 & 0x0FFF) | 0x4000; // Version 4
        let d4_variant = ((d4_high & 0x3FFF) | 0x8000) as u16; // Variant 1
        let d4_rest = d4_low & 0xFFFFFFFFFFFF;

        // Combine into a byte array
        let d4 = [
            (d4_variant >> 8) as u8,
            d4_variant as u8,
            (d4_rest >> 40) as u8,
            (d4_rest >> 32) as u8,
            (d4_rest >> 24) as u8,
            (d4_rest >> 16) as u8,
            (d4_rest >> 8) as u8,
            d4_rest as u8,
        ];

        Self(uuid::Uuid::from_fields(d1, d2, d3_version, &d4))
    }

    /// Deserialize from a memcomparable encoding.
    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let bytes = <[u8; 16]>::deserialize(deserializer)?;
        Ok(Self::from_be_bytes(bytes))
    }
}

impl UuidRef<'_> {
    /// Convert to raw bytes in little-endian order.
    #[inline]
    pub fn to_le_bytes(self) -> [u8; 16] {
        self.0.to_bytes_le()
    }

    /// Convert to raw bytes in big-endian order.
    #[inline]
    pub fn to_be_bytes(self) -> [u8; 16] {
        self.0.as_bytes().clone()
    }


    /// Serialize to protocol buffer representation.
    pub fn to_protobuf<T: std::io::Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write(&self.to_be_bytes()).map_err(Into::into)
    }

    /// Serialize to memcomparable format.
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        let bytes = self.to_be_bytes();
        bytes.serialize(serializer)
    }
}

impl ToText for UuidRef<'_> {
    fn write<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        self.write(f)
    }
}

impl ToBinary for UuidRef<'_> {
    fn to_binary_with_type(&self, _ty: &DataType) -> crate::types::to_binary::Result<Bytes> {
        let mut output = BytesMut::new();
        output.put_slice(self.0.as_bytes());
        Ok(output.freeze())
    }
}

impl EstimateSize for Uuid {
    fn estimated_heap_size(&self) -> usize {
        0 // UUIDs are fixed size and don't allocate on the heap
    }
}

// PostgreSQL compatibility
impl ToSql for Uuid {
    accepts!(UUID);

    to_sql_checked!();

    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

impl<'a> FromSql<'a> for Uuid {
    accepts!(UUID);

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("invalid UUID length".into());
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(raw);
        Ok(Self(uuid::Uuid::from_bytes(bytes)))
    }
}

impl<'a> ToSql for UuidRef<'a> {
    accepts!(UUID);

    to_sql_checked!();

    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_uuid_from_str() {
        let uuid_str = "123e4567-e89b-12d3-a456-426614174000";
        let uuid = Uuid::from_str(uuid_str).unwrap();
        assert_eq!(uuid.to_string(), uuid_str);
    }

    #[test]
    fn test_uuid_nil() {
        let nil = Uuid::nil();
        assert_eq!(nil.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_uuid_v4() {
        // Generate multiple random UUIDs to ensure uniqueness
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        // Test non-nil
        assert_ne!(uuid1, Uuid::nil());

        // Test uniqueness
        assert_ne!(uuid1, uuid2);
        assert_ne!(uuid1, uuid3);
        assert_ne!(uuid2, uuid3);

        // Test version (should be 4 for random UUIDs)
        // Extract the version nibble (13th character position of the string)
        assert_eq!(uuid1.to_string().chars().nth(14), Some('4'));
        assert_eq!(uuid2.to_string().chars().nth(14), Some('4'));
        assert_eq!(uuid3.to_string().chars().nth(14), Some('4'));

        // Test variant (should be 1 for RFC4122 UUIDs)
        // The variant is in the most significant bits of the 8th octet
        let variant1 = (uuid1.as_scalar_ref().to_be_bytes()[8] >> 6) & 0b11;
        let variant2 = (uuid2.as_scalar_ref().to_be_bytes()[8] >> 6) & 0b11;
        let variant3 = (uuid3.as_scalar_ref().to_be_bytes()[8] >> 6) & 0b11;

        assert_eq!(variant1, 0b10); // This is variant 1
        assert_eq!(variant2, 0b10);
        assert_eq!(variant3, 0b10);
    }

    #[test]
    fn test_uuid_from_arbitrary_string_builder() {
        // Test deterministic generation (same input = same UUID)
        let uuid1a = Uuid::from_arbitrary_string_builder("test_string");
        let uuid1b = Uuid::from_arbitrary_string_builder("test_string");
        assert_eq!(uuid1a, uuid1b);

        // Test uniqueness for different inputs
        let uuid2 = Uuid::from_arbitrary_string_builder("different_string");
        assert_ne!(uuid1a, uuid2);

        // Test with empty string
        let empty1 = Uuid::from_arbitrary_string_builder("");
        let empty2 = Uuid::from_arbitrary_string_builder("");
        assert_eq!(empty1, empty2);

        // Test with very long string
        let long_string = "a".repeat(1000);
        let long1 = Uuid::from_arbitrary_string_builder(&long_string);
        let long2 = Uuid::from_arbitrary_string_builder(&long_string);
        assert_eq!(long1, long2);

        // Verify version 4 (random) format is being used
        assert_eq!(uuid1a.to_string().chars().nth(14), Some('4'));

        // Test variant (should be 1 for RFC4122 UUIDs)
        let variant = (uuid1a.as_scalar_ref().to_be_bytes()[8] >> 6) & 0b11;
        assert_eq!(variant, 0b10); // This is variant 1
    }

    #[test]
    fn test_uuid_bytes_conversion() {
        let bytes = [
            0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ];
        let uuid = Uuid::from_bytes(bytes);
        assert_eq!(uuid.as_scalar_ref().to_be_bytes(), bytes);

        // Test from_le_bytes and to_le_bytes
        let le_bytes = uuid.as_scalar_ref().to_le_bytes();
        let uuid_from_le = Uuid::from_le_bytes(le_bytes);
        assert_eq!(uuid, uuid_from_le);

        // Test round-trip through u128
        let u128_value = uuid.as_u128();
        let uuid_from_u128 = Uuid::from_u128(u128_value);
        assert_eq!(uuid, uuid_from_u128);
    }

    #[test]
    fn test_uuid_from_binary() {
        let original = Uuid::new_v4();
        let bytes = original.as_scalar_ref().to_be_bytes();

        // Test from_binary
        let from_binary = Uuid::from_binary(&bytes).unwrap();
        assert_eq!(original, from_binary);

        // Test error case with insufficient bytes
        let short_bytes = &bytes[0..15];
        assert!(Uuid::from_binary(short_bytes).is_err());
    }

    #[test]
    fn test_uuid_display() {
        let uuid_str = "123e4567-e89b-12d3-a456-426614174000";
        let uuid = Uuid::from_str(uuid_str).unwrap();

        // Test Display for Uuid
        assert_eq!(format!("{}", uuid), uuid_str);

        // Test Display for UuidRef
        assert_eq!(format!("{}", uuid.as_scalar_ref()), uuid_str);
    }

    #[test]
    fn test_uuid_to_from_sql() {
        let original = Uuid::new_v4();
        let mut bytes = BytesMut::new();

        // Test to_sql
        let result = original.to_sql(&Type::UUID, &mut bytes);
        assert!(result.is_ok());

        // Test from_sql
        let from_sql = Uuid::from_sql(&Type::UUID, &bytes).unwrap();
        assert_eq!(original, from_sql);

        // Test invalid length error
        let short_bytes = &bytes[0..15];
        assert!(Uuid::from_sql(&Type::UUID, short_bytes).is_err());
    }

    #[test]
    fn test_uuid_scalar_ref() {
        let original = Uuid::new_v4();
        let ref_uuid = original.as_scalar_ref();

        // Test to_owned_scalar
        let owned = ref_uuid.to_owned_scalar();
        assert_eq!(original, owned);

        // Test hash_scalar
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        std::hash::Hash::hash(&original.0, &mut hasher1);
        ref_uuid.hash_scalar(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_uuid_estimate_size() {
        let uuid = Uuid::new_v4();
        assert_eq!(uuid.estimated_heap_size(), 0);
    }

    #[test]
    fn test_uuid_collisions() {
        // Test a large number of strings to ensure low collision rate
        let test_strings: Vec<String> = (0..1000).map(|i| format!("test_string_{}", i)).collect();

        let mut uuids = HashSet::new();
        for s in &test_strings {
            let uuid = Uuid::from_arbitrary_string_builder(s);
            uuids.insert(uuid);
        }

        // We should have very few or no collisions in 1000 different strings
        // Allow for a small number of collisions (less than 1%)
        assert!(
            uuids.len() > 990,
            "Too many UUID collisions: expected >990, got {}",
            uuids.len()
        );
    }

    #[test]
    fn test_uuid_from_str_errors() {
        // Test various invalid UUID strings
        let invalid_cases = [
            "",                                      // Empty string
            "not-a-uuid",                            // Non-UUID format
            "123e4567-e89b-12d3-a456",               // Too short
            "123e4567-e89b-12d3-a456-4266141740001", // Too long
            "123e4567-e89b-12d3-a456-42661417400g",  // Invalid character
        ];

        for invalid in &invalid_cases {
            assert!(
                Uuid::from_str(invalid).is_err(),
                "Expected error for: {}",
                invalid
            );
        }
    }
}
