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
use std::fs;
use ldap3::{LdapConnAsync, LdapError, Scope, SearchEntry};
use risingwave_common::config::{AuthMethod, HbaEntry};

use crate::error::{PsqlError, PsqlResult};

const LDAP_SERVER_KEY: &str = "ldapserver";
const LDAP_PORT_KEY: &str = "ldapport";
const LDAP_SCHEME_KEY: &str = "ldapscheme";
const LDAP_BASE_DN_KEY: &str = "ldapbasedn";
const LDAP_SEARCH_FILTER_KEY: &str = "ldapsearchfilter";

const LDAP_TLS: &str = "ldaptls";


/// LDAP configuration extracted from HBA entry
#[derive(Debug, Clone)]
pub struct LdapConfig {
    /// LDAP server address
    pub server: String,
    /// LDAP bind DN template or search base
    pub base_dn: Option<String>,
    /// LDAP search filter template
    pub search_filter: Option<String>,
    /// Whether to use STARTTLS
    pub start_tls: bool,
    /// Additional LDAP configuration options
    #[allow(dead_code)]
    pub options: HashMap<String, String>,
}

impl LdapConfig {
    /// Create LDAP configuration from HBA entry options
    pub fn from_hba_options(options: &HashMap<String, String>) -> PsqlResult<Self> {
        let server = options
            .get(LDAP_SERVER_KEY)
            .ok_or_else(|| PsqlError::StartupError("LDAP server (ldapserver) is required".into()))?
            .clone();

        let scheme = options
            .get(LDAP_SCHEME_KEY)
            .map(|s| s.as_str())
            .unwrap_or("ldap");
        if scheme != "ldap" && scheme != "ldaps" {
            return Err(PsqlError::StartupError(
                "LDAP scheme (ldapscheme) must be either 'ldap' or 'ldaps'".into(),
            ));
        }

        let port = options
            .get(LDAP_PORT_KEY)
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or_else(|| if scheme == "ldaps" { 636 } else { 389 });

        let start_tls = options
            .get(LDAP_TLS)
            .map(|s| s.as_str().parse::<bool>().unwrap())
            .unwrap_or(false);

        let server = format!("{}://{}:{}", scheme, server, port);
        let base_dn = options.get(LDAP_BASE_DN_KEY).cloned();
        let search_filter = options.get(LDAP_SEARCH_FILTER_KEY).cloned();

        Ok(Self {
            server,
            base_dn,
            search_filter,
            start_tls,
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

    /// Authenticate a user
    pub async fn authenticate(&self, username: &str, password: &str) -> PsqlResult<bool> {
        // Skip authentication if password is empty
        if password.is_empty() {
            return Ok(false);
        }

        // Determine the authentication strategy
        if self.config.base_dn.is_some() && self.config.search_filter.is_some() {
            // Search-then-bind authentication
            self.search_and_bind(username, password).await
        } else {
            // Simple bind authentication
            self.simple_bind(username, password).await
        }
    }

    /// Establish an LDAP connection with configurable options
    async fn establish_connection(&self) -> Result<ldap3::Ldap, LdapError> {
        let config = &self.config;
        let mut settings = ldap3::LdapConnSettings::new();

        // Configure STARTTLS if specified
        if config.start_tls {
            if config.server.starts_with("ldaps://") {
                return Err(LdapError::InvalidScopeString(
                    "Cannot use STARTTLS with ldaps scheme".into(),
                ));
            }
            // Question: do I need to add certificate verification connector here?
            settings = settings.set_starttls(true);
        }

        if config.server.starts_with("ldaps://") || config.start_tls {
            //FIXME: add configuration for CA certificate.
            const CA_CERT_PATH: &str = "/Users/august/Documents/codes/ldap-server/ldap/certs/ca.crt";
            let ca_cert_pem = fs::read(CA_CERT_PATH).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to read CA certificate: {}", e))
            })?;
            let ca_cert = native_tls::Certificate::from_pem(&ca_cert_pem).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to parse CA certificate: {}", e))
            })?;
            let mut builder = native_tls::TlsConnector::builder();

            // FIXME: on macOS, self-signed certificates are always rejected unless added to the system keychain.
            // We need to replace native-tls with rustls to have more control over certificate validation.
            builder.add_root_certificate(ca_cert);
            #[cfg(target_os = "macos")]
            // Accept self-signed certificates (development only)
            builder.danger_accept_invalid_certs(true);

            const CLIENT_CERT_PATH: &str = "/Users/august/Documents/codes/ldap-server/ldap/certs/client.crt";
            const CLIENT_KEY_PATH: &str = "/Users/august/Documents/codes/ldap-server/ldap/certs/client.key";

            // Read the client certificate (PEM format)
            let client_cert_pem = fs::read(CLIENT_CERT_PATH).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to read client certificate: {}", e))
            })?;

            // Read the client private key (PKCS#8 or similar format)
            let client_key_bytes = fs::read(CLIENT_KEY_PATH).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to read client private key: {}", e))
            })?;

            // Create the Identity object from the key and certificate
            let client_identity = native_tls::Identity::from_pkcs8(&client_cert_pem, &client_key_bytes)
                .map_err(|e| {
                    LdapError::InvalidScopeString(format!("Failed to parse client identity (key/cert): {}", e))
                })?;
            let builder = builder.identity(client_identity);

            let connector = builder.build().map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to build TLS connector: {}", e))
            })?;
            settings = settings.set_connector(connector);
        }

        let (conn, ldap) = LdapConnAsync::with_settings(settings, &config.server).await?;
        ldap3::drive!(conn);

        Ok(ldap)
    }

    /// Search for user in LDAP directory and then bind
    async fn search_and_bind(
        &self,
        username: &str,
        password: &str,
    ) -> PsqlResult<bool> {
        // Establish connection to LDAP server
        let mut ldap = self.establish_connection()
            .await
            .map_err(|e| {
                PsqlError::StartupError(format!("LDAP connection failed: {}", e).into())
            })?;

        // Validate base_dn and search_filter configuration
        let base_dn = self.config
            .base_dn
            .as_ref()
            .ok_or_else(|| PsqlError::StartupError("LDAP base_dn not configured".into()))?;
        let search_filter = self.config
            .search_filter
            .as_ref()
            .ok_or_else(|| PsqlError::StartupError("LDAP search_filter not configured".into()))?;

        let search_filter = search_filter.replace("{username}", username);

        let rs = ldap
            .search(base_dn, Scope::Subtree, &search_filter, vec!["dn"])
            .await
            .map_err(|e| PsqlError::StartupError(format!("LDAP search failed: {}", e).into()))?;

        // If no user found, authentication fails
        let search_entries: Vec<SearchEntry> =
            rs.0.into_iter().map(SearchEntry::construct).collect();
        if search_entries.is_empty() {
            return Ok(false);
        }

        // Attempt to bind with the user's DN and password
        let user_dn = &search_entries[0].dn;

        let bind_result = ldap
            .simple_bind(user_dn, password)
            .await
            .map_err(|e| PsqlError::StartupError(format!("LDAP bind failed: {}", e).into()));

        // Explicitly unbind the connection
        let _ = ldap.unbind().await;

        let bind_result = bind_result?;
        match bind_result.success() {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::error!(%e, "LDAP bind unsuccessful");
                Err(PsqlError::StartupError(
                    format!("LDAP bind failed: {}", e).into(),
                ))
            }
        }
    }

    /// Simple bind authentication
    async fn simple_bind(&self, username: &str, password: &str) -> PsqlResult<bool> {
        // Construct DN from username
        let dn = if let Some(base_dn) = &self.config.base_dn {
            format!("uid={},{}", username, base_dn)
        } else {
            username.to_owned()
        };

        // Attempt to bind
        let mut ldap = self.establish_connection()
            .await
            .map_err(|e| {
                PsqlError::StartupError(format!("LDAP connection failed: {}", e).into())
            })?;

        tracing::info!(%self.config.server, %dn, "simple bind authentication with LDAP server");

        let bind_result = ldap
            .simple_bind(&dn, password)
            .await
            .map_err(|e| PsqlError::StartupError(format!("LDAP bind failed: {}", e).into()));

        // Explicitly unbind the connection
        let _ = ldap.unbind().await;

        let bind_result = bind_result?;
        match bind_result.success() {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::error!(%e, "LDAP bind unsuccessful");
                Err(PsqlError::StartupError(
                    format!("LDAP bind failed: {}", e).into(),
                ))
            }
        }
    }
}
