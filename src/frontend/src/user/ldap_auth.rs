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

use std::future::Future;
use std::pin::Pin;

use ldap3::{Ldap, LdapResult};
use pgwire::error::{PsqlError, PsqlResult};
use pgwire::pg_server::LdapAuthenticator as PgwireLdapAuthenticator;
use risingwave_common::config::{AuthMethod, HbaEntry};

/// LDAP authenticator that validates user credentials against an LDAP server
#[derive(Debug, Clone)]
pub struct LdapAuthenticator {
    /// LDAP server configuration from HBA entry
    config: LdapConfig,
}

impl PgwireLdapAuthenticator for LdapAuthenticator {
    fn authenticate<'a>(
        &'a self,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = PsqlResult<bool>> + Send + 'a>> {
        let username = username.to_string();
        let password = password.to_string();
        Box::pin(async move { self.authenticate_internal(&username, &password).await })
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

    /// Internal authentication method to fix lifetime issues
    async fn authenticate_internal(&self, username: &str, password: &str) -> PsqlResult<bool> {
        // Skip authentication if password is empty
        if password.is_empty() {
            return Ok(false);
        }

        // Determine the authentication strategy
        if self.config.base_dn.is_some() && self.config.search_filter.is_some() {
            // Search-then-bind authentication
            self.authenticate_search_bind(username, password).await
        } else {
            // Simple bind authentication
            self.authenticate_simple_bind(username, password).await
        }
    }

    /// Wrapped start_tls method for LDAP connection
    async fn start_tls_connection(ldap: Ldap) -> LdapResult<Ldap> {
        // Placeholder for actual TLS configuration
        Ok(ldap)
    }

    // Rest of the implementation remains the same
}
