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

use anyhow::anyhow;
use ldap3::{LdapConnAsync, Scope, SearchEntry, dn_escape, ldap_escape};
use risingwave_common::config::{AuthMethod, HbaEntry};
use thiserror_ext::AsReport;
use tracing::warn;

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

/// LDAP TLS environment configuration
const RW_LDAPTLS_CACERT: &str = "LDAPTLS_CACERT";
const RW_LDAPTLS_CERT: &str = "LDAPTLS_CERT";
const RW_LDAPTLS_KEY: &str = "LDAPTLS_KEY";
const RW_LDAPTLS_REQCERT: &str = "LDAPTLS_REQCERT";

#[derive(Debug, Clone, Copy)]
enum ReqCertPolicy {
    Never,
    Allow,
    Try,
    Demand,
}

#[derive(Debug)]
pub struct LdapTlsConfig {
    /// `LDAPTLS_CACERT` environment variable
    ca_cert: Option<String>,
    /// `LDAPTLS_CERT` environment variable
    cert: Option<String>,
    /// `LDAPTLS_KEY` environment variable
    key: Option<String>,
    /// `LDAPTLS_REQCERT` environment variable
    req_cert: ReqCertPolicy,
}

impl LdapTlsConfig {
    /// Create LDAP TLS configuration from environment variables
    fn from_env() -> Self {
        let ca_cert = std::env::var(RW_LDAPTLS_CACERT).ok();
        let cert = std::env::var(RW_LDAPTLS_CERT).ok();
        let key = std::env::var(RW_LDAPTLS_KEY).ok();
        let req_cert = match std::env::var(RW_LDAPTLS_REQCERT).as_deref() {
            Ok("never") => ReqCertPolicy::Never,
            Ok("allow") => ReqCertPolicy::Allow,
            Ok("try") => ReqCertPolicy::Try,
            Ok("demand") => ReqCertPolicy::Demand,
            _ => ReqCertPolicy::Demand, // Default to demand
        };

        Self {
            ca_cert,
            cert,
            key,
            req_cert,
        }
    }

    /// Initialize rustls ClientConfig based on TLS configuration
    fn init_client_config(&self) -> PsqlResult<rustls::ClientConfig> {
        let tls_client_config = rustls::ClientConfig::builder().with_safe_defaults();

        let mut root_cert_store = rustls::RootCertStore::empty();
        if let Some(tls_config) = &self.ca_cert {
            let ca_cert_bytes = fs::read(tls_config).map_err(|e| {
                PsqlError::StartupError(anyhow!(e).context("Failed to read CA certificate").into())
            })?;
            let ca_certs = rustls_pemfile::certs(&mut ca_cert_bytes.as_slice()).map_err(|e| {
                PsqlError::StartupError(anyhow!(e).context("Failed to parse CA certificate").into())
            })?;
            for cert in ca_certs {
                root_cert_store
                    .add(&rustls::Certificate(cert))
                    .map_err(|err| {
                        PsqlError::StartupError(
                            anyhow!(err).context("Failed to add CA certificate").into(),
                        )
                    })?;
            }
        } else {
            // If ca certs is not present, load system native certs.
            for cert in
                rustls_native_certs::load_native_certs().expect("could not load platform certs")
            {
                root_cert_store
                    .add(&rustls::Certificate(cert.0))
                    .map_err(|err| {
                        PsqlError::StartupError(
                            anyhow!(err)
                                .context("Failed to add native CA certificate")
                                .into(),
                        )
                    })?;
            }
        }
        let tls_client_config = tls_client_config.with_root_certificates(root_cert_store);

        if let Some(cert) = &self.cert {
            let Some(key) = &self.key else {
                return Err(PsqlError::StartupError(
                    "Client certificate provided without private key".into(),
                ));
            };
            let client_cert_bytes = fs::read(cert).map_err(|e| {
                PsqlError::StartupError(
                    anyhow!(e)
                        .context("Failed to read client certificate")
                        .into(),
                )
            })?;
            let client_key_bytes = fs::read(key).map_err(|e| {
                PsqlError::StartupError(anyhow!(e).context("Failed to read client key").into())
            })?;
            let client_certs =
                rustls_pemfile::certs(&mut client_cert_bytes.as_slice()).map_err(|e| {
                    PsqlError::StartupError(
                        anyhow!(e)
                            .context("Failed to parse client certificate")
                            .into(),
                    )
                })?;

            let mut private_keys = rustls_pemfile::pkcs8_private_keys(
                &mut client_key_bytes.clone().as_slice(),
            )
            .map_err(|e| {
                PsqlError::StartupError(anyhow!(e).context("Failed to parse client key").into())
            })?;
            if private_keys.is_empty() {
                // Try RSA private keys as a fallback
                private_keys = rustls_pemfile::rsa_private_keys(&mut client_key_bytes.as_slice())
                    .map_err(|e| {
                        PsqlError::StartupError(
                            anyhow!(e).context("Failed to parse client key").into(),
                        )
                    })?;
            }
            let client_private_key = private_keys.pop().ok_or_else(|| {
                PsqlError::StartupError("No private key found in client key file".into())
            })?;
            let client_certs_rustls: Vec<rustls::Certificate> =
                client_certs.into_iter().map(rustls::Certificate).collect();

            tls_client_config
                .with_client_auth_cert(client_certs_rustls, rustls::PrivateKey(client_private_key))
                .map_err(|err| {
                    PsqlError::StartupError(
                        anyhow!(err)
                            .context("Failed to set client certificate")
                            .into(),
                    )
                })
        } else {
            Ok(tls_client_config.with_no_client_auth())
        }
    }
}

/// LDAP configuration extracted from HBA entry
#[derive(Debug, Clone)]
pub struct LdapConfig {
    /// LDAP server address
    pub server: String,
    /// LDAP bind DN template or search base
    pub base_dn: Option<String>,
    /// LDAP search filter template
    pub search_filter: Option<String>,
    /// LDAP search attribute (used in search+bind mode, defaults to "uid" if not specified)
    pub search_attribute: Option<String>,
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
}

impl LdapConfig {
    /// Create LDAP configuration from HBA entry options
    pub fn from_hba_options(options: &HashMap<String, String>) -> PsqlResult<Self> {
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
            .and_then(|p| p.parse::<bool>().ok())
            .unwrap_or(false);

        // Validate that StartTLS and ldaps are not used together
        if start_tls && scheme == "ldaps" {
            return Err(PsqlError::StartupError(
                "Cannot use STARTTLS (ldaptls) with ldaps scheme".into(),
            ));
        }

        let server = format!("{}://{}:{}", scheme, server, port);
        let base_dn = options.get(LDAP_BASE_DN_KEY).cloned();
        let search_filter = options.get(LDAP_SEARCH_FILTER_KEY).cloned();
        let search_attribute = options.get(LDAP_SEARCH_ATTRIBUTE_KEY).cloned();
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
        })
    }

    /// Parse LDAP URL (RFC 4516 format)
    /// Format: ldap\[s\]://host:port/basedn?attributes?scope?filter
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
        ];

        for param in &conflicting_params {
            if options.contains_key(*param) {
                return Err(PsqlError::StartupError(
                    format!("Cannot specify both ldapurl and {} parameter", param).into(),
                ));
            }
        }

        // Parse the URL using standard URL parsing
        let url = url::Url::parse(ldap_url).map_err(|e| {
            PsqlError::StartupError(anyhow!(e).context("Failed to parse ldap url").into())
        })?;

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
        let mut search_attribute = None;
        let mut search_filter = None;

        if let Some(query) = url.query() {
            let parts: Vec<&str> = query.split('?').collect();

            // First part is attributes (comma-separated, we only care about the first one for search)
            if !parts.is_empty() && !parts[0].is_empty() {
                search_attribute = Some(parts[0].split(',').next().unwrap().to_owned());
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
            .and_then(|p| p.parse::<bool>().ok())
            .unwrap_or(false);

        // Validate that StartTLS and ldaps are not used together
        if start_tls && scheme == "ldaps" {
            return Err(PsqlError::StartupError(
                "Cannot use STARTTLS (ldaptls) with ldaps scheme".into(),
            ));
        }

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
        // - Simple bind mode: Triggered by ldapprefix and/or ldapsuffix
        // - Search+bind mode: Triggered by ldapbasedn (when no prefix/suffix present)
        // - It's an error to mix simple bind params with search+bind-only params

        let has_simple_bind_params = self.config.prefix.is_some() || self.config.suffix.is_some();

        // Search+bind-only parameters that shouldn't be mixed with simple bind
        let has_search_only_params = self.config.search_filter.is_some()
            || self.config.bind_dn.is_some()
            || self.config.search_attribute.is_some();

        // Validate that we don't mix simple bind params with search+bind-only params
        if has_simple_bind_params && has_search_only_params {
            return Err(PsqlError::StartupError(
                "Cannot mix simple bind parameters (ldapprefix/ldapsuffix) with search+bind parameters (ldapsearchfilter/ldapbinddn/ldapsearchattribute)".into()
            ));
        }

        // Decision logic based on PostgreSQL behavior:
        // The mode is determined by which parameters are present:
        if has_simple_bind_params {
            // Simple bind mode: prefix/suffix present
            self.simple_bind(username, password).await
        } else if self.config.base_dn.is_some() {
            // Search+bind mode: basedn present without prefix/suffix
            self.search_and_bind(username, password).await
        } else {
            // Fallback: no basedn, no prefix/suffix - use username directly as DN
            self.simple_bind(username, password).await
        }
    }

    /// Establish an LDAP connection with configurable options
    #[allow(rw::format_error)]
    async fn establish_connection(&self) -> PsqlResult<ldap3::Ldap> {
        let config = &self.config;
        let mut settings = ldap3::LdapConnSettings::new();

        // Configure STARTTLS if specified
        settings = settings.set_starttls(config.start_tls);

        if config.certs_required() {
            let tls_config = LdapTlsConfig::from_env();
            tracing::debug!("fetched tls config from env: {:?}", tls_config);

            let client_config = tls_config.init_client_config()?;
            settings = settings.set_config(Arc::new(client_config));

            if matches!(tls_config.req_cert, ReqCertPolicy::Demand) {
                settings = settings.set_no_tls_verify(false);
            } else {
                warn!(
                    "LDAP client certificate verification is disabled due to LDAPTLS_REQCERT policy"
                );
                settings = settings.set_no_tls_verify(true);
            }
        }

        let (conn, ldap) = LdapConnAsync::with_settings(settings, &config.server)
            .await
            .map_err(|err| {
                PsqlError::StartupError(
                    anyhow!(err)
                        .context("Failed to connect to LDAP server")
                        .into(),
                )
            })?;
        ldap3::drive!(conn);

        Ok(ldap)
    }

    /// Search for user in LDAP directory and then bind
    async fn search_and_bind(&self, username: &str, password: &str) -> PsqlResult<bool> {
        // Establish connection to LDAP server
        let mut ldap = self.establish_connection().await?;

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
                        anyhow!(e).context("LDAP bind as search user failed").into(),
                    )
                })?
                .success()
                .map_err(|e| {
                    PsqlError::StartupError(
                        anyhow!(e).context("LDAP bind as search user failed").into(),
                    )
                })?;
        }

        // Build search filter
        let search_filter = if let Some(filter_template) = &self.config.search_filter {
            // Use custom filter template with {username} placeholder
            // SECURITY: Escape username to prevent LDAP filter injection
            let escaped_username = ldap_escape(username);
            filter_template.replace("{username}", &escaped_username)
        } else {
            // Default filter using search_attribute (defaults to "uid" if not configured)
            // SECURITY: Escape username to prevent LDAP filter injection
            let escaped_username = ldap_escape(username);
            let attr = self.config.search_attribute.as_deref().unwrap_or("uid");
            format!("({}={})", attr, escaped_username)
        };

        let rs = ldap
            .search(base_dn, Scope::Subtree, &search_filter, vec!["dn"])
            .await
            .map_err(|e| {
                PsqlError::StartupError(anyhow!(e).context("LDAP search failed").into())
            })?;

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
            .map_err(|e| PsqlError::StartupError(anyhow!(e).context("LDAP bind failed").into()));

        // Explicitly unbind the connection
        let _ = ldap.unbind().await;

        let bind_result = bind_result?;
        match bind_result.success() {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::error!(error = %e.as_report(), "LDAP bind unsuccessful");
                Err(PsqlError::StartupError(
                    anyhow!(e).context("LDAP bind failed").into(),
                ))
            }
        }
    }

    /// Simple bind authentication
    async fn simple_bind(&self, username: &str, password: &str) -> PsqlResult<bool> {
        // Construct DN from username according to PostgreSQL simple bind rules:
        // 1. If prefix/suffix are configured, use them: prefix + username + suffix
        // 2. If only basedn is configured (legacy/fallback), use: uid=username,basedn
        // 3. Otherwise, use username directly as DN
        //
        // SECURITY: Escape username to prevent LDAP DN injection
        let escaped_username = dn_escape(username);

        let dn = if self.config.prefix.is_some() || self.config.suffix.is_some() {
            // Use prefix/suffix to construct DN
            let prefix = self.config.prefix.as_deref().unwrap_or("");
            let suffix = self.config.suffix.as_deref().unwrap_or("");
            format!("{}{}{}", prefix, escaped_username, suffix)
        } else if let Some(base_dn) = &self.config.base_dn {
            // Fallback: construct DN as uid=username,basedn
            // Note: If basedn is present without prefix/suffix, this should normally
            // trigger search+bind mode, but we support this for backwards compatibility
            format!("uid={},{}", escaped_username, base_dn)
        } else {
            // Use username as-is as the DN (still escaped)
            escaped_username.to_string()
        };

        // Attempt to bind
        let mut ldap = self.establish_connection().await?;

        tracing::info!(%self.config.server, %dn, "simple bind authentication with LDAP server");

        let bind_result = ldap
            .simple_bind(&dn, password)
            .await
            .map_err(|e| PsqlError::StartupError(anyhow!(e).context("LDAP bind failed").into()));

        // Explicitly unbind the connection
        let _ = ldap.unbind().await;

        let bind_result = bind_result?;
        match bind_result.success() {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::error!(error = %e.as_report(), "LDAP bind unsuccessful");
                Err(PsqlError::StartupError(
                    format!("LDAP bind failed: {}", e.as_report()).into(),
                ))
            }
        }
    }
}
