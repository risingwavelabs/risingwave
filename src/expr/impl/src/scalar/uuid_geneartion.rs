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
use risingwave_common::types::Uuid;
use risingwave_expr::function;

/// Generate a random UUID using the gen_random_uuid() function
#[function("gen_random_uuid() -> uuid")]
pub fn gen_random_uuid() -> Uuid {
    Uuid::new_v4()
}

/// Generate a deterministic UUID from any string using the gen_uuid_from_string(varchar) function.
/// This function expects that even if the input is a valid uuid format, it will generate a new one.
#[function("gen_uuid_from_string(varchar) -> uuid")]
pub fn gen_uuid_from_string(input: &str) -> Uuid {
    Uuid::new_v5(input)
}

/// Generate a UUID from 16 bytes. Since the cast is avaiable do we need this?
#[function("gen_uuid_from_bytea(bytea) -> uuid")]
pub fn gen_uuid_from_bytea(bytes: &[u8]) -> Uuid {
    if bytes.len() == 16 {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Uuid::from_bytes(arr)
    } else {
        // a nil UUID than to panic
        Uuid::nil()
    }
}
#[cfg(test)]
mod tests {
    use risingwave_common::types::Scalar;

    use super::*;

    #[test]
    fn test_gen_random_uuid() {
        // Generate multiple UUIDs to test uniqueness
        let uuid1 = gen_random_uuid();
        let uuid2 = gen_random_uuid();

        // Test that they are not nil
        assert_ne!(uuid1, Uuid::nil());
        assert_ne!(uuid2, Uuid::nil());

        // Test uniqueness
        assert_ne!(uuid1, uuid2);

        // Test that they are valid v4 UUIDs (version 4 = random)
        let uuid_str = uuid1.to_string();
        assert_eq!(uuid_str.chars().nth(14), Some('4')); // Version should be 4

        // Test variant bits (should be 10xx for RFC 4122)
        let bytes = uuid1.as_scalar_ref().to_be_bytes();
        let variant_bits = (bytes[8] >> 6) & 0b11;
        assert_eq!(variant_bits, 0b10);
    }

    #[test]
    fn test_gen_uuid_from_string_deterministic() {
        // Test deterministic generation (same input = same UUID)
        let uuid1a = gen_uuid_from_string("test_string");
        let uuid1b = gen_uuid_from_string("test_string");
        assert_eq!(uuid1a, uuid1b);

        // Test different inputs produce different UUIDs
        let uuid2 = gen_uuid_from_string("different_string");
        assert_ne!(uuid1a, uuid2);

        // Test with empty string
        let empty1 = gen_uuid_from_string("");
        let empty2 = gen_uuid_from_string("");
        assert_eq!(empty1, empty2);
    }

    #[test]
    fn test_gen_uuid_from_bytea_valid() {
        // Test with exactly 16 bytes
        let bytes = [
            0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ];
        let uuid = gen_uuid_from_bytea(&bytes);

        // Should match the expected UUID string representation
        assert_eq!(uuid.to_string(), "123e4567-e89b-12d3-a456-426614174000");

        // Round-trip test: UUID -> bytes -> UUID
        let result_bytes = uuid.as_scalar_ref().to_be_bytes();
        assert_eq!(result_bytes, bytes);

        let round_trip_uuid = gen_uuid_from_bytea(&result_bytes);
        assert_eq!(uuid, round_trip_uuid);
    }

    #[test]
    fn test_function_integration() {
        // Test that different functions produce different results
        let random_uuid = gen_random_uuid();
        let string_uuid = gen_uuid_from_string("test");
        let bytes_uuid = gen_uuid_from_bytea(&[0x12u8; 16]);

        // They should all be different (extremely high probability)
        assert_ne!(random_uuid, string_uuid);
        assert_ne!(random_uuid, bytes_uuid);
        assert_ne!(string_uuid, bytes_uuid);

        // None should be nil (unless specifically created as such)
        assert_ne!(random_uuid, Uuid::nil());
        assert_ne!(string_uuid, Uuid::nil());
        assert_ne!(bytes_uuid, Uuid::nil());
    }

    #[test]
    fn test_performance_characteristics() {
        // This test ensures functions don't panic and can handle rapid calls
        for _ in 0..100 {
            let _ = gen_random_uuid();
            let _ = gen_uuid_from_string("test");
            let _ = gen_uuid_from_bytea(&[0x12u8; 16]);
        }
    }
}
