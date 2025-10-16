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

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// RisingWave HBA (Host-Based Authentication) configuration, similar to PostgreSQL's `pg_hba.conf`
/// This determines which authentication method to use for each connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HbaConfig {
    pub entries: Vec<HbaEntry>,
}

impl Default for HbaConfig {
    fn default() -> Self {
        Self {
            entries: vec![
                // Default rule: allow all local connections without authentication
                HbaEntry {
                    connection_type: ConnectionType::Local,
                    databases: vec!["all".to_owned()],
                    users: vec!["all".to_owned()],
                    addresses: None,
                    auth_method: AuthMethod::Trust,
                    auth_options: HashMap::new(),
                },
                // Default rule: require password for all remote connections
                HbaEntry {
                    connection_type: ConnectionType::Host,
                    databases: vec!["all".to_owned()],
                    users: vec!["all".to_owned()],
                    addresses: Some(vec![AddressPattern::Cidr("0.0.0.0/0".to_owned())]),
                    auth_method: AuthMethod::Password,
                    auth_options: HashMap::new(),
                },
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HbaEntry {
    /// Connection type (local, host, hostssl, hostnossl)
    pub connection_type: ConnectionType,
    /// Database names or "all"
    pub databases: Vec<String>,
    /// Usernames or "all"
    pub users: Vec<String>,
    /// Client addresses (only for non-local connections)
    pub addresses: Option<Vec<AddressPattern>>,
    /// Authentication method
    pub auth_method: AuthMethod,
    /// Authentication method options
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub auth_options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConnectionType {
    /// Unix socket connections
    Local,
    /// TCP/IP connections (both SSL and non-SSL)
    Host,
    /// TCP/IP connections that use SSL
    #[serde(rename = "hostssl")]
    HostSsl,
    /// TCP/IP connections that do not use SSL
    #[serde(rename = "hostnossl")]
    HostNoSsl,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddressPattern {
    /// IP address with CIDR notation (e.g., "192.168.1.0/24")
    Cidr(String),
    /// Hostname
    Hostname(String),
    /// Keyword "all"
    All,
}

impl Serialize for AddressPattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            AddressPattern::All => serializer.serialize_str("all"),
            AddressPattern::Cidr(s) => serializer.serialize_str(s),
            AddressPattern::Hostname(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for AddressPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "all" {
            Ok(AddressPattern::All)
        } else if s.contains('/') {
            Ok(AddressPattern::Cidr(s))
        } else {
            Ok(AddressPattern::Hostname(s))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AuthMethod {
    /// No authentication
    #[serde(rename = "trust")]
    Trust,
    /// Clear-text password
    #[serde(rename = "password")]
    Password,
    /// MD5-hashed password
    #[serde(rename = "md5")]
    Md5,
    /// LDAP authentication
    #[serde(rename = "ldap")]
    Ldap,
    /// OAuth/JWT authentication
    #[serde(rename = "oauth")]
    OAuth,
}

impl HbaConfig {
    /// Find the first matching HBA entry for the given connection parameters
    pub fn find_matching_entry(
        &self,
        connection_type: &ConnectionType,
        database: &str,
        user: &str,
        client_addr: Option<&IpAddr>,
        _is_ssl: bool, // TODO: implement SSL detection
    ) -> Option<&HbaEntry> {
        self.entries
            .iter()
            .find(|entry| self.matches_entry(entry, connection_type, database, user, client_addr))
    }

    fn matches_entry(
        &self,
        entry: &HbaEntry,
        connection_type: &ConnectionType,
        database: &str,
        user: &str,
        client_addr: Option<&IpAddr>,
    ) -> bool {
        // Check connection type
        if !self.matches_connection_type(&entry.connection_type, connection_type) {
            return false;
        }

        // Check database
        if !self.matches_list(&entry.databases, database) {
            return false;
        }

        // Check user
        if !self.matches_list(&entry.users, user) {
            return false;
        }

        // Check address (only for non-local connections)
        if *connection_type != ConnectionType::Local
            && let Some(addresses) = &entry.addresses
        {
            if let Some(addr) = client_addr {
                if !self.matches_address(addresses, addr) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    fn matches_connection_type(
        &self,
        entry_type: &ConnectionType,
        actual_type: &ConnectionType,
    ) -> bool {
        match (entry_type, actual_type) {
            (ConnectionType::Local, ConnectionType::Local) => true,
            (ConnectionType::Host, ConnectionType::Host) => true,
            (ConnectionType::Host, ConnectionType::HostSsl) => true,
            (ConnectionType::Host, ConnectionType::HostNoSsl) => true,
            (ConnectionType::HostSsl, ConnectionType::HostSsl) => true,
            (ConnectionType::HostNoSsl, ConnectionType::HostNoSsl) => true,
            _ => false,
        }
    }

    fn matches_list(&self, list: &[String], value: &str) -> bool {
        list.iter().any(|item| item == "all" || item == value)
    }

    fn matches_address(&self, patterns: &[AddressPattern], addr: &IpAddr) -> bool {
        patterns.iter().any(|pattern| match pattern {
            AddressPattern::All => true,
            AddressPattern::Cidr(cidr) => self.matches_cidr(cidr, addr),
            AddressPattern::Hostname(_hostname) => {
                // TODO: implement hostname resolution
                false
            }
        })
    }

    fn matches_cidr(&self, cidr: &str, addr: &IpAddr) -> bool {
        if let Ok(network) = ipnet::IpNet::from_str(cidr) {
            network.contains(addr)
        } else {
            false
        }
    }

    /// Load HBA configuration from a TOML string
    pub fn from_toml(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(content)
    }

    /// Save HBA configuration to a TOML string
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}

impl FromStr for HbaConfig {
    type Err = toml::de::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_toml(s)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[test]
    fn test_default_hba_config() {
        let config = HbaConfig::default();
        assert_eq!(config.entries.len(), 2);

        // Test local connection
        let entry =
            config.find_matching_entry(&ConnectionType::Local, "testdb", "testuser", None, false);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().auth_method, AuthMethod::Trust);
    }

    #[test]
    fn test_cidr_matching() {
        let config = HbaConfig::default();
        let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));

        assert!(config.matches_cidr("192.168.1.0/24", &addr));
        assert!(!config.matches_cidr("10.0.0.0/8", &addr));
    }

    #[test]
    fn test_ldap_config_serialization() {
        let mut auth_options = HashMap::new();
        auth_options.insert("ldapserver".to_string(), "ldap.example.com".to_string());
        auth_options.insert("ldapport".to_string(), "389".to_string());
        auth_options.insert("ldapprefix".to_string(), "cn=".to_string());
        auth_options.insert("ldapsuffix".to_string(), ",dc=example,dc=com".to_string());

        let entry = HbaEntry {
            connection_type: ConnectionType::Host,
            databases: vec!["all".to_string()],
            users: vec!["all".to_string()],
            addresses: Some(vec![AddressPattern::Cidr("10.0.0.0/8".to_string())]),
            auth_method: AuthMethod::Ldap,
            auth_options,
        };

        let config = HbaConfig {
            entries: vec![entry],
        };

        let toml_str = config.to_toml().unwrap();
        let parsed_config = HbaConfig::from_toml(&toml_str).unwrap();

        assert_eq!(parsed_config.entries.len(), 1);
        assert_eq!(parsed_config.entries[0].auth_method, AuthMethod::Ldap);
        assert_eq!(
            parsed_config.entries[0].auth_options.get("ldapserver"),
            Some(&"ldap.example.com".to_string())
        );
    }
}
