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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use ldap3::{LdapConn, Scope, SearchEntry};
use pgwire::error::{PsqlError, PsqlResult};
use pgwire::pg_server::LdapAuthenticator as PgwireLdapAuthenticator;
use risingwave_common::config::{AuthMethod, HbaEntry};

/// LDAP configuration extracted from HBA entry
#[derive(Debug, Clone)]
pub struct LdapConfig {
    /// LDAP server address
    pub server: String,
    /// LDAP bind DN template or search base
    pub base_dn: Option<String>,
    /// LDAP search filter template
    pub search_filter: Option<String>,
    /// Additional LDAP configuration options
    pub options: HashMap<String, String>,
}

impl LdapConfig {
    /// Create LDAP configuration from HBA entry options
    pub fn from_hba_options(options: &HashMap<String, String>) -> PsqlResult<Self> {
        let server = options
            .get("host")
            .ok_or_else(|| PsqlError::StartupError("LDAP host is required".into()))?
            .clone();

        Ok(Self {
            server,
            base_dn: options.get("base_dn").cloned(),
            search_filter: options.get("search_filter").cloned(),
            options: options.clone(),
        })
    }
}

/// LDAP authenticator that validates user credentials against an LDAP server
#[derive(Debug, Clone)]
pub struct LdapAuthenticator {
    /// LDAP server configuration from HBA entry
    config: LdapConfig,
}

impl PgwireLdapAuthenticator for LdapAuthenticator {
    fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Pin<Box<dyn Future<Output = PsqlResult<bool>> + Send>> {
        let username = username.to_string();
        let password = password.to_string();
        let config = self.config.clone();
        Box::pin(async move {
            // Skip authentication if password is empty
            if password.is_empty() {
                return Ok(false);
            }

            // Determine the authentication strategy
            if config.base_dn.is_some() && config.search_filter.is_some() {
                // Search-then-bind authentication
                LdapAuthenticator::search_and_bind(&config, &username, &password).await
            } else {
                // Simple bind authentication
                LdapAuthenticator::simple_bind(&config, &username, &password).await
            }
        })
    }
}

impl LdapAuthenticator {
    /// Create a new LDAP authenticator from HBA entry options
    pub fn new(hba_entry: &HbaEntry) -> PsqlResult<Self> {
        if hba_entry.auth_method != AuthMethod::Ldap {
            return Err(PsqlError::StartupError(
                "HBA entry is not configured for LDAP authentication".into(),
            ));
        }

        let config = LdapConfig::from_hba_options(&hba_entry.auth_options)?;
        Ok(Self { config })
    }

    /// Search for user in LDAP directory and then bind
    async fn search_and_bind(
        config: &LdapConfig,
        username: &str,
        password: &str,
    ) -> PsqlResult<bool> {
        // Establish connection to LDAP server
        let mut conn = LdapConn::new(&config.server).map_err(|e| {
            PsqlError::StartupError(format!("LDAP connection failed: {}", e).into())
        })?;

        // Search for the user with the provided filter
        let base_dn = config.base_dn.as_ref().unwrap();
        let search_filter = config
            .search_filter
            .as_ref()
            .unwrap()
            .replace("{username}", username);

        let rs = conn
            .search(base_dn, Scope::Subtree, &search_filter, vec!["dn"])
            .map_err(|e| PsqlError::StartupError(format!("LDAP search failed: {}", e).into()))?;

        // If no user found, authentication fails
        let search_entries: Vec<SearchEntry> =
            rs.0.into_iter().map(SearchEntry::construct).collect();
        if search_entries.is_empty() {
            return Ok(false);
        }

        // Attempt to bind with the user's DN and password
        let user_dn = &search_entries[0].dn;

        conn.simple_bind(user_dn, password)
            .map_err(|e| PsqlError::StartupError(format!("LDAP bind failed: {}", e).into()))
            .map(|_| true)
    }

    /// Simple bind authentication
    async fn simple_bind(config: &LdapConfig, username: &str, password: &str) -> PsqlResult<bool> {
        // Construct DN from username
        let dn = if let Some(base_dn) = &config.base_dn {
            format!("uid={},{}", username, base_dn)
        } else {
            username.to_string()
        };

        // Attempt to bind
        let mut conn = LdapConn::new(&config.server).map_err(|e| {
            PsqlError::StartupError(format!("LDAP connection failed: {}", e).into())
        })?;

        conn.simple_bind(&dn, password)
            .map_err(|e| PsqlError::StartupError(format!("LDAP bind failed: {}", e).into()))
            .map(|_| true)
    }
}
