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

//! OpenPGP packet definitions and parsing
//!
//! This module implements OpenPGP packet structures according to RFC 4880,
//! including packet headers, literal data packets, symmetric key encrypted
//! session key packets, and other packet types needed for PGP operations.

use std::fmt::Debug;

use chrono::Utc;
use risingwave_expr::{ExprError, Result};

use super::pgp_impl::{CipherAlgo, CompressionAlgo, HashAlgo, S2kMode, invalid_param_error};

/// OpenPGP packet types (from RFC 4880)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PacketType {
    Reserved = 0,
    PublicKeyEncryptedSessionKey = 1,
    Signature = 2,
    SymmetricKeyEncryptedSessionKey = 3,
    OnePassSignature = 4,
    SecretKey = 5,
    PublicKey = 6,
    SecretSubkey = 7,
    CompressedData = 8,
    SymmetricKeyEncryptedData = 9,
    Marker = 10,
    LiteralData = 11,
    Trust = 12,
    UserId = 13,
    PublicSubkey = 14,
    UserAttribute = 17,
    SymmetricEncryptedIntegrityProtectedData = 18,
    ModificationDetectionCode = 19,
}

impl PacketType {
    fn from_u8(n: u8) -> Result<Self> {
        match n {
            0 => Ok(Self::Reserved),
            1 => Ok(Self::PublicKeyEncryptedSessionKey),
            2 => Ok(Self::Signature),
            3 => Ok(Self::SymmetricKeyEncryptedSessionKey),
            4 => Ok(Self::OnePassSignature),
            5 => Ok(Self::SecretKey),
            6 => Ok(Self::PublicKey),
            7 => Ok(Self::SecretSubkey),
            8 => Ok(Self::CompressedData),
            9 => Ok(Self::SymmetricKeyEncryptedData),
            10 => Ok(Self::Marker),
            11 => Ok(Self::LiteralData),
            12 => Ok(Self::Trust),
            13 => Ok(Self::UserId),
            14 => Ok(Self::PublicSubkey),
            17 => Ok(Self::UserAttribute),
            18 => Ok(Self::SymmetricEncryptedIntegrityProtectedData),
            19 => Ok(Self::ModificationDetectionCode),
            _ => Err(invalid_param_error(
                "packet-type",
                &format!("unknown packet type: {}", n),
            )),
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            Self::Reserved => 0,
            Self::PublicKeyEncryptedSessionKey => 1,
            Self::Signature => 2,
            Self::SymmetricKeyEncryptedSessionKey => 3,
            Self::OnePassSignature => 4,
            Self::SecretKey => 5,
            Self::PublicKey => 6,
            Self::SecretSubkey => 7,
            Self::CompressedData => 8,
            Self::SymmetricKeyEncryptedData => 9,
            Self::Marker => 10,
            Self::LiteralData => 11,
            Self::Trust => 12,
            Self::UserId => 13,
            Self::PublicSubkey => 14,
            Self::UserAttribute => 17,
            Self::SymmetricEncryptedIntegrityProtectedData => 18,
            Self::ModificationDetectionCode => 19,
        }
    }
}

/// OpenPGP packet header
#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub packet_type: PacketType,
    pub length: usize,
    pub partial: bool,
}

impl PacketHeader {
    /// Parse packet header from data stream
    pub fn parse(data: &[u8]) -> Result<(Self, usize)> {
        if data.is_empty() {
            return Err(ExprError::InvalidParam {
                name: "packet-header",
                reason: "empty data".into(),
            });
        }

        let first_byte = data[0];
        let tag = (first_byte & 0x3C) >> 2;
        let format = first_byte & 0x03;

        let packet_type = Self::from_u8(tag)?;

        let (length, consumed) = match format {
            0 => {
                // Old format: bits 0-1 are format, bits 2-7 are tag
                // Length is in next byte
                if data.len() < 2 {
                    return Err(ExprError::InvalidParam {
                        name: "packet-header",
                        reason: "incomplete old format header".into(),
                    });
                }
                (data[1] as usize, 2)
            }
            1 => {
                // Partial body length (first of potentially multiple packets)
                if data.len() < 2 {
                    return Err(ExprError::InvalidParam {
                        name: "packet-header",
                        reason: "incomplete partial header".into(),
                    });
                }
                let length_byte = data[1];
                let length = match length_byte {
                    0..=191 => length_byte as usize,
                    192..=223 => {
                        // Two-octet length
                        if data.len() < 3 {
                            return Err(ExprError::InvalidParam {
                                name: "packet-header",
                                reason: "incomplete two-octet length".into(),
                            });
                        }
                        let high = (length_byte as usize - 192) * 256;
                        let low = data[2] as usize;
                        high + low + 192
                    }
                    224..=254 => {
                        // Four-octet length (but we don't support it fully)
                        return Err(ExprError::InvalidParam {
                            name: "packet-header",
                            reason: "four-octet length not supported".into(),
                        });
                    }
                    255 => {
                        // Five-octet length (but we don't support it fully)
                        return Err(ExprError::InvalidParam {
                            name: "packet-header",
                            reason: "five-octet length not supported".into(),
                        });
                    }
                };
                (length, if length_byte < 192 { 2 } else { 3 })
            }
            2 => {
                // New format: bits 0-1 are format, bits 2-7 are tag
                // Length encoding follows
                if data.len() < 2 {
                    return Err(ExprError::InvalidParam {
                        name: "packet-header",
                        reason: "incomplete new format header".into(),
                    });
                }
                let length_byte = data[1];
                let (length, additional_bytes) = match length_byte {
                    0..=191 => (length_byte as usize, 0),
                    192..=223 => {
                        // Two-octet length
                        if data.len() < 3 {
                            return Err(ExprError::InvalidParam {
                                name: "packet-header",
                                reason: "incomplete two-octet length".into(),
                            });
                        }
                        let high = (length_byte as usize - 192) * 256;
                        let low = data[2] as usize;
                        (high + low + 192, 1)
                    }
                    224..=254 => {
                        // Four-octet length (but we don't support it fully)
                        return Err(ExprError::InvalidParam {
                            name: "packet-header",
                            reason: "four-octet length not supported".into(),
                        });
                    }
                    255 => {
                        // Indeterminate length (but we don't support it fully)
                        return Err(ExprError::InvalidParam {
                            name: "packet-header",
                            reason: "indeterminate length not supported".into(),
                        });
                    }
                };
                (length, 2 + additional_bytes)
            }
            _ => unreachable!(),
        };

        Ok((
            Self {
                packet_type,
                length,
                partial: format == 1,
            },
            consumed,
        ))
    }
}

/// Base packet trait
pub trait Packet: Debug {
    fn packet_type(&self) -> PacketType;
    fn serialize(&self) -> Result<Vec<u8>>;
}

/// Literal data packet (RFC 4880 Section 5.9)
#[derive(Debug, Clone)]
pub struct LiteralDataPacket {
    pub format: u8, // 'b' for binary, 't' for text, 'u' for UTF-8 text
    pub filename: String,
    pub timestamp: u32,
    pub data: Vec<u8>,
}

impl LiteralDataPacket {
    pub fn new_binary(data: Vec<u8>) -> Self {
        Self {
            format: b'b',
            filename: String::new(),
            timestamp: Utc::now().timestamp() as u32,
            data,
        }
    }

    pub fn new_text(data: Vec<u8>, filename: String) -> Self {
        Self {
            format: b't',
            filename,
            timestamp: Utc::now().timestamp() as u32,
            data,
        }
    }

    pub fn new_utf8(data: Vec<u8>, filename: String) -> Self {
        Self {
            format: b'u',
            filename,
            timestamp: Utc::now().timestamp() as u32,
            data,
        }
    }

    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 6 {
            return Err(ExprError::InvalidParam {
                name: "literal-data",
                reason: "packet too short".into(),
            });
        }

        let format = data[0];
        let filename_len = data[1] as usize;
        let mut offset = 2;

        if data.len() < offset + filename_len + 4 {
            return Err(ExprError::InvalidParam {
                name: "literal-data",
                reason: "packet too short for filename".into(),
            });
        }

        let filename = String::from_utf8_lossy(&data[offset..offset + filename_len]).to_string();
        offset += filename_len;

        let timestamp = u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let literal_data = data[offset..].to_vec();

        Ok(Self {
            format,
            filename,
            timestamp,
            data: literal_data,
        })
    }
}

impl Packet for LiteralDataPacket {
    fn packet_type(&self) -> PacketType {
        PacketType::LiteralData
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        // Format (1 byte)
        result.push(self.format);

        // Filename length (1 byte)
        let filename_bytes = self.filename.as_bytes();
        result.push(filename_bytes.len() as u8);

        // Filename
        result.extend_from_slice(filename_bytes);

        // Timestamp (4 bytes, big-endian)
        result.extend_from_slice(&self.timestamp.to_be_bytes());

        // Data
        result.extend_from_slice(&self.data);

        Ok(result)
    }
}

/// Symmetric-Key Encrypted Session Key packet (RFC 4880 Section 5.3)
#[derive(Debug, Clone)]
pub struct SymmetricKeyEncryptedSessionKeyPacket {
    pub version: u8,
    pub cipher_algo: CipherAlgo,
    pub s2k_mode: S2kMode,
    pub s2k_digest: HashAlgo,
    pub s2k_salt: Option<[u8; 8]>,
    pub s2k_count: Option<u8>,
    pub iv: Vec<u8>,
    pub encrypted_key: Vec<u8>,
}

impl SymmetricKeyEncryptedSessionKeyPacket {
    pub fn new(
        cipher_algo: CipherAlgo,
        s2k_mode: S2kMode,
        s2k_digest: HashAlgo,
        s2k_salt: Option<[u8; 8]>,
        s2k_count: Option<u8>,
        session_key: &[u8],
        passphrase: &[u8],
    ) -> Result<Self> {
        let iv_size = cipher_algo.block_size();
        let mut iv = vec![0u8; iv_size];
        openssl::rand::rand_bytes(&mut iv)?;

        // For simplicity, we'll use a simple encryption scheme here
        // In a full implementation, this would use proper OpenPGP encryption
        let encrypted_key = session_key.to_vec(); // Placeholder

        Ok(Self {
            version: 4, // OpenPGP version 4
            cipher_algo,
            s2k_mode,
            s2k_digest,
            s2k_salt,
            s2k_count,
            iv,
            encrypted_key,
        })
    }

    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(ExprError::InvalidParam {
                name: "skesk",
                reason: "empty data".into(),
            });
        }

        let version = data[0];
        if version != 4 {
            return Err(ExprError::InvalidParam {
                name: "skesk",
                reason: format!("unsupported version: {}", version).into(),
            });
        }

        let cipher_algo_byte = data[1];
        let cipher_algo = match cipher_algo_byte {
            7 => CipherAlgo::Aes128,
            8 => CipherAlgo::Aes192,
            9 => CipherAlgo::Aes256,
            _ => {
                return Err(ExprError::InvalidParam {
                    name: "skesk",
                    reason: format!("unsupported cipher algorithm: {}", cipher_algo_byte).into(),
                });
            }
        };

        let s2k_mode = S2kMode::from(data[2]);
        let s2k_digest = HashAlgo::from(data[3])?;

        let mut offset = 4;

        let (s2k_salt, s2k_count) = match s2k_mode {
            S2kMode::Simple => (None, None),
            S2kMode::Salted => {
                if data.len() < offset + 8 {
                    return Err(ExprError::InvalidParam {
                        name: "skesk",
                        reason: "missing salt for salted S2K".into(),
                    });
                }
                let salt = data[offset..offset + 8].try_into().unwrap();
                offset += 8;
                (Some(salt), None)
            }
            S2kMode::IteratedAndSalted => {
                if data.len() < offset + 9 {
                    return Err(ExprError::InvalidParam {
                        name: "skesk",
                        reason: "missing salt and count for iterated S2K".into(),
                    });
                }
                let salt = data[offset..offset + 8].try_into().unwrap();
                offset += 8;
                let count = data[offset];
                offset += 1;
                (Some(salt), Some(count))
            }
        };

        // For simplicity, skip IV and encrypted key parsing
        // In a full implementation, this would decrypt the session key

        Ok(Self {
            version,
            cipher_algo,
            s2k_mode,
            s2k_digest,
            s2k_salt,
            s2k_count,
            iv: Vec::new(),
            encrypted_key: Vec::new(),
        })
    }
}

impl Packet for SymmetricKeyEncryptedSessionKeyPacket {
    fn packet_type(&self) -> PacketType {
        PacketType::SymmetricKeyEncryptedSessionKey
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        // Version (1 byte)
        result.push(self.version);

        // Cipher algorithm (1 byte)
        let cipher_byte = match self.cipher_algo {
            CipherAlgo::Aes128 => 7,
            CipherAlgo::Aes192 => 8,
            CipherAlgo::Aes256 => 9,
            _ => {
                return Err(ExprError::InvalidParam {
                    name: "skesk",
                    reason: "unsupported cipher algorithm for serialization".into(),
                });
            }
        };
        result.push(cipher_byte);

        // S2K mode (1 byte)
        result.push(self.s2k_mode as u8);

        // S2K digest algorithm (1 byte)
        let digest_byte = match self.s2k_digest {
            HashAlgo::Md5 => 1,
            HashAlgo::Sha1 => 2,
            HashAlgo::Sha256 => 8,
            _ => {
                return Err(ExprError::InvalidParam {
                    name: "skesk",
                    reason: "unsupported hash algorithm for serialization".into(),
                });
            }
        };
        result.push(digest_byte);

        // S2K salt (8 bytes if present)
        if let Some(salt) = self.s2k_salt {
            result.extend_from_slice(&salt);
        }

        // S2K count (1 byte if present)
        if let Some(count) = self.s2k_count {
            result.push(count);
        }

        // For simplicity, skip IV and encrypted key in serialization
        // In a full implementation, this would include proper encryption

        Ok(result)
    }
}

/// Symmetric-Key Encrypted Data packet (RFC 4880 Section 5.7)
#[derive(Debug, Clone)]
pub struct SymmetricKeyEncryptedDataPacket {
    pub version: u8,
    pub encrypted_data: Vec<u8>,
}

impl Packet for SymmetricKeyEncryptedDataPacket {
    fn packet_type(&self) -> PacketType {
        PacketType::SymmetricKeyEncryptedData
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        result.push(self.version);
        result.extend_from_slice(&self.encrypted_data);
        Ok(result)
    }
}

/// Compressed Data packet (RFC 4880 Section 5.6)
#[derive(Debug, Clone)]
pub struct CompressedDataPacket {
    pub algorithm: CompressionAlgo,
    pub data: Vec<u8>,
}

impl Packet for CompressedDataPacket {
    fn packet_type(&self) -> PacketType {
        PacketType::CompressedData
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        result.push(self.algorithm as u8);
        result.extend_from_slice(&self.data);
        Ok(result)
    }
}

/// Modification Detection Code packet (RFC 4880 Section 5.14)
#[derive(Debug, Clone)]
pub struct ModificationDetectionCodePacket {
    pub hash: Vec<u8>,
}

impl Packet for ModificationDetectionCodePacket {
    fn packet_type(&self) -> PacketType {
        PacketType::ModificationDetectionCode
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        Ok(self.hash.clone())
    }
}
