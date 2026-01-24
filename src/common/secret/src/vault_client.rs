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
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use moka::future::Cache as MokaCache;
use reqwest::Client;
use risingwave_pb::secret;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TokenCacheKey {
    vault_base_url: String,
    role_id: String,
}

#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

/// Global cache for Vault tokens to reduce authentication requests
/// Cache key contains (vault service base url, `role_id`) as requested
static GLOBAL_VAULT_TOKEN_CACHE: LazyLock<MokaCache<TokenCacheKey, CachedToken>> =
    LazyLock::new(|| {
        MokaCache::builder()
            .max_capacity(1000) // Limit cache size
            .build()
    });

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashiCorpVaultConfig {
    pub addr: String,
    pub path: String,
    pub field: String,
    #[serde(flatten)]
    pub auth: HashiCorpVaultAuth,
    #[serde(default)]
    #[serde_as(as = "DisplayFromStr")]
    pub tls_skip_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "auth_method", rename_all = "lowercase")]
pub enum HashiCorpVaultAuth {
    Token {
        auth_token: String,
    },
    #[serde(rename = "approle")]
    AppRole {
        auth_role_id: String,
        auth_secret_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultAppRoleLoginRequest {
    role_id: String,
    secret_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultAuthResponse {
    auth: VaultAuthData,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultAuthData {
    client_token: String,
    lease_duration: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultSecretResponse {
    data: VaultSecretData,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultSecretData {
    data: HashMap<String, Value>,
}

#[derive(Debug)]
pub struct HashiCorpVaultClient {
    client: Client,
    config: HashiCorpVaultConfig,
}

impl HashiCorpVaultConfig {
    /// Convert from protobuf `SecretHashicorpVaultBackend` to `HashiCorpVaultConfig`
    pub fn from_protobuf(vault_backend: &secret::SecretHashicorpVaultBackend) -> Result<Self> {
        let auth = match vault_backend.auth.as_ref() {
            Some(secret::secret_hashicorp_vault_backend::Auth::TokenAuth(token_auth)) => {
                HashiCorpVaultAuth::Token {
                    auth_token: token_auth.token.clone(),
                }
            }
            Some(secret::secret_hashicorp_vault_backend::Auth::ApproleAuth(approle_auth)) => {
                HashiCorpVaultAuth::AppRole {
                    auth_role_id: approle_auth.role_id.clone(),
                    auth_secret_id: approle_auth.secret_id.clone(),
                }
            }
            None => {
                return Err(anyhow::anyhow!(
                    "No auth method specified for Vault backend"
                ));
            }
        };

        Ok(HashiCorpVaultConfig {
            addr: vault_backend.addr.clone(),
            path: vault_backend.path.clone(),
            field: vault_backend.field.clone(),
            auth,
            tls_skip_verify: vault_backend.tls_skip_verify,
        })
    }

    /// Convert `HashiCorpVaultConfig` to protobuf `SecretHashicorpVaultBackend`
    pub fn to_protobuf(&self) -> secret::SecretHashicorpVaultBackend {
        let auth = match &self.auth {
            HashiCorpVaultAuth::Token { auth_token } => Some(
                secret::secret_hashicorp_vault_backend::Auth::TokenAuth(secret::VaultTokenAuth {
                    token: auth_token.clone(),
                }),
            ),
            HashiCorpVaultAuth::AppRole {
                auth_role_id,
                auth_secret_id,
            } => Some(secret::secret_hashicorp_vault_backend::Auth::ApproleAuth(
                secret::VaultAppRoleAuth {
                    role_id: auth_role_id.clone(),
                    secret_id: auth_secret_id.clone(),
                },
            )),
        };

        secret::SecretHashicorpVaultBackend {
            addr: self.addr.clone(),
            path: self.path.clone(),
            field: self.field.clone(),
            auth,
            tls_skip_verify: self.tls_skip_verify,
        }
    }
}

impl HashiCorpVaultClient {
    pub fn new(config: HashiCorpVaultConfig) -> Result<Self> {
        let mut client_builder = Client::builder();

        if config.tls_skip_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        let client = client_builder
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self { client, config })
    }

    pub async fn get_secret(&self) -> Result<Vec<u8>> {
        // Try to get secret, with retry logic for token invalidation
        let mut force_refresh_token = false;

        // Retry loop for handling token invalidation
        for retry_count in 0..2 {
            // Get token (either directly or via app role)
            let token = self.get_token_internal(force_refresh_token).await?;

            // Fetch secret from Vault
            let mut url = Url::parse(&self.config.addr).context("Failed to parse Vault address")?;
            url.set_path(&format!("v1/{}", self.config.path));
            let response = self
                .client
                .get(url.as_str())
                .header("X-Vault-Token", &token)
                .send()
                .await
                .context("Failed to send request to Vault")?;

            // Handle authentication failures - token may have been rotated/revoked
            if (response.status() == 401 || response.status() == 403)
                && retry_count == 0
                && matches!(self.config.auth, HashiCorpVaultAuth::AppRole { .. })
            {
                // this case means the token changed during cache, need to trigger a refresh
                force_refresh_token = true;
                continue;
            }

            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "Vault API returned error status: {} - {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ));
            }

            // Success case - process the response and break out of retry loop
            return self.process_secret_response(response).await;
        }

        // todo: refine error message
        Err(anyhow::anyhow!("Failed to get secret from Vault"))
    }

    async fn process_secret_response(&self, response: reqwest::Response) -> Result<Vec<u8>> {
        // https://developer.hashicorp.com/vault/docs/secrets/kv/kv-v2/cookbook/read-data
        // a demo response:
        //   {
        //     "request_id": "e345b77b-8b5a-552b-eb2c-7d80a627c9ad",
        //     "lease_id": "",
        //     "renewable": false,
        //     "lease_duration": 0,
        //     "data": {
        //       "data": {
        //         "key": "test-api-key-12345",
        //         "secret": "test-api-secret-67890"
        //       },
        //       "metadata": {
        //         "created_time": "2025-07-17T08:07:24.177261949Z",
        //         "custom_metadata": null,
        //         "deletion_time": "",
        //         "destroyed": false,
        //         "version": 1
        //       }
        //     },
        //     "wrap_info": null,
        //     "warnings": null,
        //     "auth": null,
        //     "mount_type": "kv"
        //   }

        let secret_response: VaultSecretResponse = response
            .json()
            .await
            .context("Failed to parse Vault secret response")?;

        let field_value = secret_response
            .data
            .data
            .get(&self.config.field)
            .ok_or_else(|| anyhow::anyhow!("Field '{}' not found in secret", self.config.field))?;

        let secret_bytes = match field_value {
            Value::String(s) => s.as_bytes().to_vec(),
            _ => serde_json::to_vec(field_value)
                .context("Failed to serialize field value to bytes")?,
        };

        Ok(secret_bytes)
    }

    async fn get_token_internal(&self, force_refresh: bool) -> Result<String> {
        match &self.config.auth {
            HashiCorpVaultAuth::Token { auth_token } => Ok(auth_token.clone()),
            HashiCorpVaultAuth::AppRole {
                auth_role_id,
                auth_secret_id,
            } => {
                // Create cache key with vault base URL and role_id
                let cache_key = TokenCacheKey {
                    vault_base_url: self.config.addr.trim_end_matches('/').to_owned(),
                    role_id: auth_role_id.clone(),
                };

                // Check global token cache first (unless forced refresh)
                if !force_refresh
                    && let Some(cached_token) = GLOBAL_VAULT_TOKEN_CACHE.get(&cache_key).await
                {
                    if cached_token.expires_at > Instant::now() {
                        return Ok(cached_token.token);
                    } else {
                        // Token expired, remove it from cache
                        GLOBAL_VAULT_TOKEN_CACHE.invalidate(&cache_key).await;
                    }
                }

                // Login with app role
                let mut login_url =
                    Url::parse(&self.config.addr).context("Failed to parse Vault address")?;
                login_url.set_path("v1/auth/approle/login");
                let login_request = VaultAppRoleLoginRequest {
                    role_id: auth_role_id.clone(),
                    secret_id: auth_secret_id.clone(),
                };

                let response = self
                    .client
                    .post(login_url.as_str())
                    .json(&login_request)
                    .send()
                    .await
                    .context("Failed to send app role login request")?;

                if !response.status().is_success() {
                    // If authentication fails and we have a cached token, invalidate it
                    if !force_refresh {
                        GLOBAL_VAULT_TOKEN_CACHE.invalidate(&cache_key).await;
                    }
                    return Err(anyhow::anyhow!(
                        "Vault app role login failed: {} - {}",
                        response.status(),
                        response.text().await.unwrap_or_default()
                    ));
                }

                let auth_response: VaultAuthResponse = response
                    .json()
                    .await
                    .context("Failed to parse Vault auth response")?;

                let token = auth_response.auth.client_token;
                let lease_duration = auth_response.auth.lease_duration;

                // Cache the token with per-entry expiration based on lease duration (90% of lease duration)
                let expires_at = Instant::now() + Duration::from_secs((lease_duration * 9) / 10);
                let cached_token = CachedToken {
                    token: token.clone(),
                    expires_at,
                };
                GLOBAL_VAULT_TOKEN_CACHE
                    .insert(cache_key, cached_token)
                    .await;

                Ok(token)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_hashicorp_vault_config_token_auth_full() {
        let json_config = json!({
            "addr": "https://vault.example.com",
            "path": "secret/data/myapp",
            "field": "api_key",
            "auth_method": "token",
            "auth_token": "hvs.123abc",
            "tls_skip_verify": "true"
        });

        let config: HashiCorpVaultConfig = serde_json::from_value(json_config).unwrap();

        assert_eq!(config.addr, "https://vault.example.com");
        assert_eq!(config.path, "secret/data/myapp");
        assert_eq!(config.field, "api_key");
        assert!(config.tls_skip_verify);

        match config.auth {
            HashiCorpVaultAuth::Token { auth_token } => {
                assert_eq!(auth_token, "hvs.123abc");
            }
            _ => panic!("Expected Token auth method"),
        }
    }

    #[test]
    fn test_hashicorp_vault_config_approle_auth_full() {
        let json_config = json!({
            "addr": "https://vault.example.com",
            "path": "secret/data/myapp",
            "field": "password",
            "auth_method": "approle",
            "auth_role_id": "role123",
            "auth_secret_id": "secret456",
            "tls_skip_verify": "false"
        });

        let config: HashiCorpVaultConfig = serde_json::from_value(json_config).unwrap();

        assert_eq!(config.addr, "https://vault.example.com");
        assert_eq!(config.path, "secret/data/myapp");
        assert_eq!(config.field, "password");
        assert!(!config.tls_skip_verify);

        match config.auth {
            HashiCorpVaultAuth::AppRole {
                auth_role_id,
                auth_secret_id,
            } => {
                assert_eq!(auth_role_id, "role123");
                assert_eq!(auth_secret_id, "secret456");
            }
            _ => panic!("Expected AppRole auth method"),
        }
    }
}
