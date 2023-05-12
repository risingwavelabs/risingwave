// Copyright 2023 RisingWave Labs
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

use risingwave_expr_macro::function;
use sha1::{Digest, Sha1};
use sha2::{Sha224, Sha256, Sha384, Sha512};

use crate::Result;

#[function("sha1(bytea) -> bytea")]
pub fn sha1(data: &[u8]) -> Result<Box<[u8]>> {
    Ok(Sha1::digest(data).to_vec().into())
}

#[function("sha224(bytea) -> bytea")]
pub fn sha224(data: &[u8]) -> Result<Box<[u8]>> {
    Ok(Sha224::digest(data).to_vec().into())
}

#[function("sha256(bytea) -> bytea")]
pub fn sha256(data: &[u8]) -> Result<Box<[u8]>> {
    Ok(Sha256::digest(data).to_vec().into())
}

#[function("sha384(bytea) -> bytea")]
pub fn sha384(data: &[u8]) -> Result<Box<[u8]>> {
    Ok(Sha384::digest(data).to_vec().into())
}

#[function("sha512(bytea) -> bytea")]
pub fn sha512(data: &[u8]) -> Result<Box<[u8]>> {
    Ok(Sha512::digest(data).to_vec().into())
}

#[cfg(test)]
mod tests {
    use super::{sha1, sha224, sha256, sha384, sha512};
    #[test]
    fn test_sha1() {
        let cases = [(
            r#"hello world"#.as_bytes(),
            b"\x2a\xae\x6c\x35\xc9\x4f\xcf\xb4\x15\xdb\xe9\x5f\x40\x8b\x9c\xe9\x1e\xe8\x46\xed",
        )];

        for (ori, encoded) in cases {
            let t = sha1(ori).unwrap();
            let slice: &[u8] = &t;
            assert_eq!(slice, encoded);
        }
    }

    #[test]
    fn test_sha224() {
        let cases = [
            (r#"hello world"#.as_bytes(), b"\x2f\x05\x47\x7f\xc2\x4b\xb4\xfa\xef\xd8\x65\x17\x15\x6d\xaf\xde\xce\xc4\x5b\x8a\xd3\xcf\x25\x22\xa5\x63\x58\x2b"),
        ];

        for (ori, encoded) in cases {
            let t = sha224(ori).unwrap();
            let slice: &[u8] = &t;
            assert_eq!(slice, encoded);
        }
    }

    #[test]
    fn test_sha256() {
        let cases = [
            (r#"hello world"#.as_bytes(), b"\xb9\x4d\x27\xb9\x93\x4d\x3e\x08\xa5\x2e\x52\xd7\xda\x7d\xab\xfa\xc4\x84\xef\xe3\x7a\x53\x80\xee\x90\x88\xf7\xac\xe2\xef\xcd\xe9"),
        ];

        for (ori, encoded) in cases {
            let t = sha256(ori).unwrap();
            let slice: &[u8] = &t;
            assert_eq!(slice, encoded);
        }
    }

    #[test]
    fn test_sha384() {
        let cases = [
            (r#"hello world"#.as_bytes(), b"\xfd\xbd\x8e\x75\xa6\x7f\x29\xf7\x01\xa4\xe0\x40\x38\x5e\x2e\x23\x98\x63\x03\xea\x10\x23\x92\x11\xaf\x90\x7f\xcb\xb8\x35\x78\xb3\xe4\x17\xcb\x71\xce\x64\x6e\xfd\x08\x19\xdd\x8c\x08\x8d\xe1\xbd"),
        ];

        for (ori, encoded) in cases {
            let t = sha384(ori).unwrap();
            let slice: &[u8] = &t;
            assert_eq!(slice, encoded);
        }
    }

    #[test]
    fn test_sha512() {
        let cases = [
            (r#"hello world"#.as_bytes(), b"\x30\x9e\xcc\x48\x9c\x12\xd6\xeb\x4c\xc4\x0f\x50\xc9\x02\xf2\xb4\xd0\xed\x77\xee\x51\x1a\x7c\x7a\x9b\xcd\x3c\xa8\x6d\x4c\xd8\x6f\x98\x9d\xd3\x5b\xc5\xff\x49\x96\x70\xda\x34\x25\x5b\x45\xb0\xcf\xd8\x30\xe8\x1f\x60\x5d\xcf\x7d\xc5\x54\x2e\x93\xae\x9c\xd7\x6f"),
        ];

        for (ori, encoded) in cases {
            let t = sha512(ori).unwrap();
            let slice: &[u8] = &t;
            assert_eq!(slice, encoded);
        }
    }
}
