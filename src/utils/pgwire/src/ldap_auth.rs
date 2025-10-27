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
use std::sync::Arc;

use ldap3::{LdapConnAsync, LdapError, Scope, SearchEntry};
use risingwave_common::config::{AuthMethod, HbaEntry};
use thiserror_ext::AsReport;

use crate::error::{PsqlError, PsqlResult};

const LDAP_SERVER_KEY: &str = "ldapserver";
const LDAP_PORT_KEY: &str = "ldapport";
const LDAP_SCHEME_KEY: &str = "ldapscheme";
const LDAP_BASE_DN_KEY: &str = "ldapbasedn";
const LDAP_SEARCH_FILTER_KEY: &str = "ldapsearchfilter";
const LDAP_SEARCH_ATTRIBUTE_KEY: &str = "ldapsearchattribute";
const LDAP_BIND_DN_KEY: &str = "ldapbinddn";
const LDAP_BIND_PASSWD_KEY: &str = "ldapbindpasswd";
const LDAP_PREFIX_KEY: &str = "ldapprefix";
const LDAP_SUFFIX_KEY: &str = "ldapsuffix";
const LDAP_URL_KEY: &str = "ldapurl";

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
    /// LDAP search attribute (defaults to "uid")
    pub search_attribute: String,
    /// DN to bind as when performing searches
    pub bind_dn: Option<String>,
    /// Password for bind DN
    pub bind_passwd: Option<String>,
    /// Prefix to prepend to username in simple bind
    pub prefix: Option<String>,
    /// Suffix to append to username in simple bind
    pub suffix: Option<String>,
    /// Whether to use STARTTLS
    pub start_tls: bool,
    /// Additional LDAP configuration options
    #[allow(dead_code)]
    pub options: HashMap<String, String>,
}

impl LdapConfig {
    /// Create LDAP configuration from HBA entry options
    pub fn from_hba_options(options: &HashMap<String, String>) -> PsqlResult<Self> {
        // Check if ldapurl is provided - it takes precedence over individual parameters
        if let Some(ldap_url) = options.get(LDAP_URL_KEY) {
            return Self::from_ldap_url(ldap_url, options);
        }

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
        let search_attribute = options
            .get(LDAP_SEARCH_ATTRIBUTE_KEY)
            .cloned()
            .unwrap_or_else(|| "uid".to_owned());
        let bind_dn = options.get(LDAP_BIND_DN_KEY).cloned();
        let bind_passwd = options.get(LDAP_BIND_PASSWD_KEY).cloned();
        let prefix = options.get(LDAP_PREFIX_KEY).cloned();
        let suffix = options.get(LDAP_SUFFIX_KEY).cloned();

        Ok(Self {
            server,
            base_dn,
            search_filter,
            search_attribute,
            bind_dn,
            bind_passwd,
            prefix,
            suffix,
            start_tls,
            options: options.clone(),
        })
    }

    /// Parse LDAP URL (RFC 4516 format)
    /// Format: ldap[s]://host:port/basedn?attributes?scope?filter
    fn from_ldap_url(ldap_url: &str, options: &HashMap<String, String>) -> PsqlResult<Self> {
        // Validate that conflicting parameters are not present
        // According to PostgreSQL docs, ldapurl cannot be mixed with parameters that would conflict
        let conflicting_params = [
            LDAP_SERVER_KEY,
            LDAP_PORT_KEY,
            LDAP_SCHEME_KEY,
            LDAP_BASE_DN_KEY,
            LDAP_SEARCH_ATTRIBUTE_KEY,
            LDAP_SEARCH_FILTER_KEY,
            LDAP_PREFIX_KEY,
            LDAP_SUFFIX_KEY,
        ];

        for param in &conflicting_params {
            if options.contains_key(*param) {
                return Err(PsqlError::StartupError(
                    format!("Cannot specify both ldapurl and {} parameter", param).into(),
                ));
            }
        }

        // Parse the URL using standard URL parsing
        let url = url::Url::parse(ldap_url)
            .map_err(|e| PsqlError::StartupError(format!("Invalid LDAP URL: {}", e).into()))?;

        // Validate scheme
        let scheme = url.scheme();
        if scheme != "ldap" && scheme != "ldaps" {
            return Err(PsqlError::StartupError(
                "LDAP URL scheme must be either 'ldap' or 'ldaps'".into(),
            ));
        }

        // Extract host and port
        let host = url
            .host_str()
            .ok_or_else(|| PsqlError::StartupError("LDAP URL must contain a host".into()))?;
        let port = url
            .port()
            .unwrap_or_else(|| if scheme == "ldaps" { 636 } else { 389 });

        let server = format!("{}://{}:{}", scheme, host, port);

        // Extract basedn from path (remove leading /)
        let base_dn = if url.path().len() > 1 {
            Some(url.path()[1..].to_string())
        } else {
            None
        };

        // Parse query parameters for attributes, scope, filter
        // Format: ?attributes?scope?filter
        let mut search_attribute = "uid".to_owned();
        let mut search_filter = None;

        if let Some(query) = url.query() {
            let parts: Vec<&str> = query.split('?').collect();

            // First part is attributes (comma-separated, we only care about the first one for search)
            if !parts.is_empty() && !parts[0].is_empty() {
                search_attribute = parts[0].split(',').next().unwrap_or("uid").to_owned();
            }

            // Third part is filter (index 2)
            if parts.len() > 2 && !parts[2].is_empty() {
                search_filter = Some(parts[2].to_owned());
            }
        }

        // Only allow supplementary parameters with ldapurl:
        // - ldaptls: for StartTLS
        // - ldapbinddn/ldapbindpasswd: for authenticated searches
        let start_tls = options
            .get(LDAP_TLS)
            .map(|s| s.as_str().parse::<bool>().unwrap())
            .unwrap_or(false);

        let bind_dn = options.get(LDAP_BIND_DN_KEY).cloned();
        let bind_passwd = options.get(LDAP_BIND_PASSWD_KEY).cloned();

        // prefix and suffix are not allowed with ldapurl
        let prefix = None;
        let suffix = None;

        Ok(Self {
            server,
            base_dn,
            search_filter,
            search_attribute,
            bind_dn,
            bind_passwd,
            prefix,
            suffix,
            start_tls,
            options: options.clone(),
        })
    }
    
    fn certs_required(&self) -> bool {
        self.server.starts_with("ldaps://") || self.start_tls
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

        // Determine the authentication strategy based on configured parameters
        // According to PostgreSQL documentation:
        // - Simple bind mode: Uses ldapprefix and/or ldapsuffix
        // - Search+bind mode: Uses ldapbasedn with optional search parameters
        // - It's an error to mix both modes

        let has_simple_bind_params = self.config.prefix.is_some() || self.config.suffix.is_some();
        let has_search_bind_params = self.config.search_filter.is_some()
            || self.config.bind_dn.is_some()
            || self.config.search_attribute != "uid";

        // Validate that we don't mix simple bind and search+bind parameters
        if has_simple_bind_params && has_search_bind_params {
            return Err(PsqlError::StartupError(
                "Cannot mix simple bind parameters (ldapprefix/ldapsuffix) with search+bind parameters (ldapsearchfilter/ldapbinddn/ldapsearchattribute)".into()
            ));
        }

        if has_simple_bind_params {
            // Simple bind mode: use prefix/suffix to construct DN
            self.simple_bind(username, password).await
        } else if self.config.base_dn.is_some() {
            // Search+bind mode: search for user in directory then bind
            self.search_and_bind(username, password).await
        } else {
            // No basedn, use username directly as DN
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
            settings = settings.set_starttls(true);
        }

        if config.certs_required() {
            // FIXME: add configuration for CA certificate.
            // RisingWave does not have parameters like `ldap_ca_file`, `ldap_cert_file`, or `ldap_key_file`.
            // PostgreSQL itself does not provide these options. Instead, it uses the libldap (OpenLDAP client library) for LDAP connections and authentication.
            //     TLS certificate parameters such as `TLS_CACERT`, `TLS_CERT`, and `TLS_KEY` are configured in the libldap configuration file, not in PostgreSQL's configuration.
            //     When PostgreSQL starts and performs LDAP authentication, its process follows this lookup order for certificate configuration:
            //
            // 1. Environment Variables (highest priority): PostgreSQL inherits the environment variables from its startup environment.
            //     - `LDAPTLS_CACERT` → replaces `TLS_CACERT`
            //     - `LDAPTLS_CERT` → replaces `TLS_CERT`
            //     - `LDAPTLS_KEY` → replaces `TLS_KEY`
            //     - `LDAPTLS_REQCERT` → replaces `TLS_REQCERT`
            //    - Example:
            //      ```
            //      export LDAPTLS_CACERT=/etc/openldap/certs/ca.pem
            //      export LDAPTLS_CERT=/etc/openldap/certs/postgres.crt
            //      export LDAPTLS_KEY=/etc/openldap/certs/postgres.key
            //      export LDAPTLS_REQCERT=demand
            //      ```
            //
            // 2. Configuration File: `/etc/openldap/ldap.conf`
            //
            // 3. System Default Directories:
            // - `/etc/ssl/certs/`
            // - macOS system trust chain

            const CA_CERT_PATH: &str =
                "/Users/august/Documents/codes/ldap-server/ldap/certs/ca.crt";
            let ca_cert_bytes = fs::read(CA_CERT_PATH).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to read CA certificate: {}", e))
            })?;
            let ca_certs = rustls_pemfile::certs(&mut ca_cert_bytes.as_slice()).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to parse CA certificates: {}", e))
            })?;

            const CLIENT_CERT_PATH: &str =
                "/Users/august/Documents/codes/ldap-server/ldap/certs/client.crt";
            const CLIENT_KEY_PATH: &str =
                "/Users/august/Documents/codes/ldap-server/ldap/certs/client.key";

            let client_cert_bytes = fs::read(CLIENT_CERT_PATH).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to read client certificate: {}", e))
            })?;

            let client_key_bytes = fs::read(CLIENT_KEY_PATH).map_err(|e| {
                LdapError::InvalidScopeString(format!("Failed to read client private key: {}", e))
            })?;

            let client_certs =
                rustls_pemfile::certs(&mut client_cert_bytes.as_slice()).map_err(|e| {
                    LdapError::InvalidScopeString(format!(
                        "Failed to parse client certificates: {}",
                        e
                    ))
                })?;

            let mut private_keys = rustls_pemfile::pkcs8_private_keys(
                &mut client_key_bytes.as_slice(),
            )
            .map_err(|e| {
                LdapError::InvalidScopeString(format!(
                    "Failed to parse client private key: {}",
                    e.as_report()
                ))
            })?;

            let client_private_key = private_keys.pop().ok_or_else(|| {
                LdapError::InvalidScopeString("No private key found in client key file".into())
            })?;

            let mut root_cert_store = rustls::RootCertStore::empty();
            for cert in ca_certs {
                root_cert_store
                    .add(&rustls::Certificate(cert))
                    .map_err(|err| {
                        LdapError::InvalidScopeString(format!(
                            "Failed to add CA certificate to root store: {}",
                            err.as_report()
                        ))
                    })?;
            }
            // If ca certs is not present, load system native certs.
            for cert in
                rustls_native_certs::load_native_certs().expect("could not load platform certs")
            {
                root_cert_store
                    .add(&rustls::Certificate(cert.0))
                    .map_err(|err| {
                        LdapError::InvalidScopeString(format!(
                            "Failed to add native certificate to root store: {}",
                            err.as_report()
                        ))
                    })?;
            }

            let client_certs_rustls: Vec<rustls::Certificate> =
                client_certs.into_iter().map(rustls::Certificate).collect();

            let config = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_cert_store)
                .with_client_auth_cert(client_certs_rustls, rustls::PrivateKey(client_private_key))
                .map_err(|err| {
                    LdapError::InvalidScopeString(format!(
                        "Failed to build TLS config: {}",
                        err.as_report()
                    ))
                })?;
            settings = settings.set_config(Arc::new(config));
        }

        let (conn, ldap) = LdapConnAsync::with_settings(settings, &config.server).await?;
        ldap3::drive!(conn);

        Ok(ldap)
    }

    /// Search for user in LDAP directory and then bind
    async fn search_and_bind(&self, username: &str, password: &str) -> PsqlResult<bool> {
        // Establish connection to LDAP server
        let mut ldap = self.establish_connection().await.map_err(|e| {
            PsqlError::StartupError(format!("LDAP connection failed: {}", e).into())
        })?;

        // Validate base_dn configuration
        let base_dn = self
            .config
            .base_dn
            .as_ref()
            .ok_or_else(|| PsqlError::StartupError("LDAP base_dn not configured".into()))?;

        // If bind_dn and bind_passwd are provided, bind as that user first
        if let (Some(bind_dn), Some(bind_passwd)) = (&self.config.bind_dn, &self.config.bind_passwd)
        {
            ldap.simple_bind(bind_dn, bind_passwd)
                .await
                .map_err(|e| {
                    PsqlError::StartupError(
                        format!("LDAP bind as search user failed: {}", e).into(),
                    )
                })?
                .success()
                .map_err(|e| {
                    PsqlError::StartupError(
                        format!("LDAP bind as search user failed: {}", e).into(),
                    )
                })?;
        }

        // Build search filter
        let search_filter = if let Some(filter_template) = &self.config.search_filter {
            // Use custom filter template with {username} placeholder
            filter_template.replace("{username}", username)
        } else {
            // Default filter using search_attribute
            format!("({}={})", self.config.search_attribute, username)
        };

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
        // Construct DN from username with optional prefix and suffix
        let dn = if let Some(base_dn) = &self.config.base_dn {
            // Use base_dn with optional prefix/suffix
            let prefix = self.config.prefix.as_deref().unwrap_or("uid=");
            let suffix = self.config.suffix.as_deref().unwrap_or("");

            if suffix.is_empty() {
                format!("{}{},{}", prefix, username, base_dn)
            } else {
                format!("{}{}{}", prefix, username, suffix)
            }
        } else if self.config.prefix.is_some() || self.config.suffix.is_some() {
            // Use only prefix and suffix without base_dn
            let prefix = self.config.prefix.as_deref().unwrap_or("");
            let suffix = self.config.suffix.as_deref().unwrap_or("");
            format!("{}{}{}", prefix, username, suffix)
        } else {
            // Use username as-is
            username.to_owned()
        };

        // Attempt to bind
        let mut ldap = self.establish_connection().await.map_err(|e| {
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
