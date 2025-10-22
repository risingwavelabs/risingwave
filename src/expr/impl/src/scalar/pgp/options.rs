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

//! PGP options parsing and handling
//!
//! This module implements parsing and handling of PGP options as specified
//! in PostgreSQL pgcrypto documentation. Options are passed as comma-separated
//! key-value pairs in a text string format.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use risingwave_expr::{ExprError, Result};

use super::pgp_impl::{CipherAlgo, CompressionAlgo, HashAlgo, ParseFromStr, ParseFromU8, S2kMode};

/// Represents a parsed PGP option value
#[derive(Debug, Clone)]
pub enum PgpOptionValue {
    String(String),
    Integer(i32),
    Boolean(bool),
}

// Using standard Default trait instead of custom HasDefault

/// PGP options container
#[derive(Debug, Clone)]
pub struct PgpOptions {
    options: HashMap<String, PgpOptionValue>,
}

impl PgpOptions {
    /// Parse options from a comma-separated string
    pub fn parse(options_str: &str) -> Result<Self> {
        let mut options = HashMap::new();

        if options_str.trim().is_empty() {
            return Ok(Self { options });
        }

        for option in options_str.split(',') {
            let option = option.trim();
            if option.is_empty() {
                continue;
            }

            if let Some((key, value)) = option.split_once('=') {
                let key = key.trim().to_string();
                let value = value.trim();

                let parsed_value = if value == "true" || value == "false" {
                    PgpOptionValue::Boolean(value == "true")
                } else if let Ok(int_val) = value.parse::<i32>() {
                    PgpOptionValue::Integer(int_val)
                } else {
                    PgpOptionValue::String(value.to_string())
                };

                options.insert(key, parsed_value);
            } else {
                // Boolean option without value (defaults to true)
                options.insert(option.to_string(), PgpOptionValue::Boolean(true));
            }
        }

        Ok(Self { options })
    }

    /// Get cipher algorithm option
    pub fn get_cipher_algo(&self) -> Result<CipherAlgo> {
        if let Some(PgpOptionValue::String(s)) = self.options.get("cipher-algo") {
            CipherAlgo::parse_from_str(s)
        } else {
            Ok(CipherAlgo::default_value())
        }
    }

    /// Get compression algorithm option
    pub fn get_compression_algo(&self) -> Result<CompressionAlgo> {
        if let Some(PgpOptionValue::Integer(n)) = self.options.get("compress-algo") {
            CompressionAlgo::parse_from_u8(*n as u8)
        } else if let Some(PgpOptionValue::String(s)) = self.options.get("compress-algo") {
            match s.as_str() {
                "0" | "none" => Ok(CompressionAlgo::Uncompressed),
                "1" | "zip" => Ok(CompressionAlgo::Zip),
                "2" | "zlib" => Ok(CompressionAlgo::Zlib),
                _ => Err(ExprError::InvalidParam {
                    name: "compress-algo",
                    reason: format!("invalid compression algorithm: {}", s).into(),
                }),
            }
        } else {
            Ok(CompressionAlgo::default())
        }
    }

    /// Get S2K mode option
    pub fn get_s2k_mode(&self) -> Result<S2kMode> {
        if let Some(PgpOptionValue::Integer(n)) = self.options.get("s2k-mode") {
            S2kMode::parse_from_u8(*n as u8)
        } else if let Some(PgpOptionValue::String(s)) = self.options.get("s2k-mode") {
            S2kMode::parse_from_str(s)
        } else {
            Ok(S2kMode::default())
        }
    }

    /// Get S2K digest algorithm option
    pub fn get_s2k_digest(&self) -> Result<HashAlgo> {
        if let Some(PgpOptionValue::Integer(n)) = self.options.get("s2k-digest-algo") {
            HashAlgo::parse_from_u8(*n as u8)
        } else if let Some(PgpOptionValue::String(s)) = self.options.get("s2k-digest-algo") {
            HashAlgo::parse_from_str(s)
        } else {
            Ok(HashAlgo::default())
        }
    }

    /// Get S2K salt option (8 bytes)
    pub fn get_s2k_salt(&self) -> Option<[u8; 8]> {
        if let Some(PgpOptionValue::String(s)) = self.options.get("s2k-salt") {
            // Parse hex string
            if s.len() == 16 {
                let mut salt = [0u8; 8];
                for (i, byte) in s.as_bytes().chunks(2).enumerate() {
                    if i >= 8 {
                        break;
                    }
                    if let Ok(val) = u8::from_str_radix(std::str::from_utf8(byte).unwrap(), 16) {
                        salt[i] = val;
                    } else {
                        return None;
                    }
                }
                Some(salt)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get S2K iterations option
    pub fn get_s2k_iterations(&self) -> Option<u32> {
        if let Some(PgpOptionValue::Integer(n)) = self.options.get("s2k-iterations") {
            Some(*n as u32)
        } else if let Some(PgpOptionValue::String(s)) = self.options.get("s2k-iterations") {
            s.parse::<u32>().ok()
        } else {
            None
        }
    }

    /// Get unicode mode option
    pub fn get_unicode_mode(&self) -> Option<bool> {
        if let Some(PgpOptionValue::Boolean(b)) = self.options.get("unicode-mode") {
            Some(*b)
        } else {
            None
        }
    }

    /// Get convert CRLF option
    pub fn get_convert_crlf(&self) -> Option<bool> {
        if let Some(PgpOptionValue::Boolean(b)) = self.options.get("convert-crlf") {
            Some(*b)
        } else {
            None
        }
    }

    /// Get session key option
    pub fn get_sess_key(&self) -> Option<bool> {
        if let Some(PgpOptionValue::Boolean(b)) = self.options.get("sess-key") {
            Some(*b)
        } else {
            None
        }
    }

    /// Get armor option
    pub fn get_armor(&self) -> Option<bool> {
        if let Some(PgpOptionValue::Boolean(b)) = self.options.get("armor") {
            Some(*b)
        } else {
            None
        }
    }

    /// Get disable MDC option
    pub fn get_disable_mdc(&self) -> Option<bool> {
        if let Some(PgpOptionValue::Boolean(b)) = self.options.get("disable-mdc") {
            Some(*b)
        } else {
            None
        }
    }

    /// Get raw option (for bytea functions)
    pub fn get_raw(&self) -> Option<bool> {
        if let Some(PgpOptionValue::Boolean(b)) = self.options.get("raw") {
            Some(*b)
        } else {
            None
        }
    }

    /// Get an option value with a default
    pub fn get_with_default<T: Default>(&self, key: &str) -> T {
        T::default()
    }

    /// Get an option value that can be parsed from string
    pub fn get_parsed<T: ParseFromStr>(&self, key: &str) -> Result<T> {
        if let Some(PgpOptionValue::String(s)) = self.options.get(key) {
            T::parse_from_str(s)
        } else {
            Ok(T::default())
        }
    }

    /// Get an option value that can be parsed from u8
    pub fn get_parsed_u8<T: ParseFromU8>(&self, key: &str) -> Result<T> {
        if let Some(PgpOptionValue::Integer(n)) = self.options.get(key) {
            T::parse_from_u8(*n as u8)
        } else {
            Ok(T::default())
        }
    }

    /// Validate that all options are known and valid
    pub fn validate(&self) -> Result<()> {
        let known_options = [
            "cipher-algo",
            "compress-algo",
            "unicode-mode",
            "convert-crlf",
            "sess-key",
            "armor",
            "s2k-mode",
            "s2k-digest-algo",
            "s2k-cipher-algo",
            "s2k-salt",
            "s2k-iterations",
            "disable-mdc",
            "raw",
            "unicode-armor",
        ];

        for (key, _) in &self.options {
            if !known_options.contains(&key.as_str()) {
                return Err(ExprError::InvalidParam {
                    name: "options",
                    reason: format!("unknown option: {}", key).into(),
                });
            }
        }

        Ok(())
    }

    /// Get all options as a hashmap for debugging
    pub fn get_all(&self) -> &HashMap<String, PgpOptionValue> {
        &self.options
    }
}

impl Default for PgpOptions {
    fn default() -> Self {
        Self {
            options: HashMap::new(),
        }
    }
}

/// Regex for parsing option strings
static OPTION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([^=,]+)(?:=([^,]*))?$").unwrap());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_options() {
        let opts = PgpOptions::parse("").unwrap();
        assert!(opts.get_all().is_empty());
    }

    #[test]
    fn test_parse_single_option() {
        let opts = PgpOptions::parse("cipher-algo=aes256").unwrap();
        assert_eq!(opts.get_cipher_algo().unwrap(), CipherAlgo::Aes256);
    }

    #[test]
    fn test_parse_multiple_options() {
        let opts = PgpOptions::parse("cipher-algo=aes256,compress-algo=1,armor=true").unwrap();
        assert_eq!(opts.get_cipher_algo().unwrap(), CipherAlgo::Aes256);
        assert_eq!(opts.get_compression_algo().unwrap(), CompressionAlgo::Zip);
        assert_eq!(opts.get_armor().unwrap(), true);
    }

    #[test]
    fn test_parse_boolean_option() {
        let opts = PgpOptions::parse("unicode-mode").unwrap();
        assert_eq!(opts.get_unicode_mode().unwrap(), true);
    }

    #[test]
    fn test_parse_s2k_salt() {
        let opts = PgpOptions::parse("s2k-salt=0123456789abcdef").unwrap();
        let salt = opts.get_s2k_salt().unwrap();
        assert_eq!(salt, [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    }

    #[test]
    fn test_invalid_option() {
        let result = PgpOptions::parse("invalid-option=value");
        assert!(result.is_err());
    }
}
