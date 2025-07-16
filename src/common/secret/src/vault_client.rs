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
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct VaultConfig {
    pub addr: String,
    pub path: String,
    pub field: String,
    pub auth: VaultAuth,
    pub tls_skip_verify: bool,
    pub cache_ttl_secs: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum VaultAuth {
    Token { token: String },
    AppRole { role_id: String, secret_id: String },
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
struct CachedSecret {
    value: Vec<u8>,
    expires_at: Instant,
}

#[derive(Debug)]
pub struct VaultClient {
    client: Client,
    config: VaultConfig,
    token_cache: Mutex<Option<(String, Instant)>>,
    secret_cache: Mutex<HashMap<String, CachedSecret>>,
}

impl VaultClient {
    pub fn new(config: VaultConfig) -> Result<Self> {
        let mut client_builder = Client::builder();

        if config.tls_skip_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        let client = client_builder
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            config,
            token_cache: Mutex::new(None),
            secret_cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn get_secret(&self) -> Result<Vec<u8>> {
        // Check cache first if TTL is configured
        if let Some(_ttl_secs) = self.config.cache_ttl_secs {
            let cache_key = format!("{}#{}", self.config.path, self.config.field);
            let mut cache = self.secret_cache.lock();

            if let Some(cached) = cache.get(&cache_key) {
                if cached.expires_at > Instant::now() {
                    return Ok(cached.value.clone());
                } else {
                    cache.remove(&cache_key);
                }
            }
        }

        // Get token (either directly or via app role)
        let token = self.get_token().await?;

        // Fetch secret from Vault
        let url = format!(
            "{}/v1/{}",
            self.config.addr.trim_end_matches('/'),
            self.config.path
        );
        let response = self
            .client
            .get(&url)
            .header("X-Vault-Token", &token)
            .send()
            .await
            .context("Failed to send request to Vault")?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Vault API returned error status: {} - {}",
                response.status(),
                response.text().await.unwrap_or_default()
            ));
        }

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

        // Cache the result if TTL is configured
        if let Some(ttl_secs) = self.config.cache_ttl_secs {
            let cache_key = format!("{}#{}", self.config.path, self.config.field);
            let expires_at = Instant::now() + Duration::from_secs(ttl_secs as u64);
            let cached = CachedSecret {
                value: secret_bytes.clone(),
                expires_at,
            };

            let mut cache = self.secret_cache.lock();
            cache.insert(cache_key, cached);
        }

        Ok(secret_bytes)
    }

    async fn get_token(&self) -> Result<String> {
        match &self.config.auth {
            VaultAuth::Token { token } => Ok(token.clone()),
            VaultAuth::AppRole { role_id, secret_id } => {
                // Check token cache first
                {
                    let cache = self.token_cache.lock();
                    if let Some((token, expires_at)) = &*cache
                        && *expires_at > Instant::now()
                    {
                        return Ok(token.clone());
                    }
                }

                // Login with app role
                let login_url = format!(
                    "{}/v1/auth/approle/login",
                    self.config.addr.trim_end_matches('/')
                );
                let login_request = VaultAppRoleLoginRequest {
                    role_id: role_id.clone(),
                    secret_id: secret_id.clone(),
                };

                let response = self
                    .client
                    .post(&login_url)
                    .json(&login_request)
                    .send()
                    .await
                    .context("Failed to send app role login request")?;

                if !response.status().is_success() {
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

                // Cache the token with some buffer time (90% of lease duration)
                let expires_at = Instant::now() + Duration::from_secs((lease_duration * 9) / 10);
                {
                    let mut cache = self.token_cache.lock();
                    *cache = Some((token.clone(), expires_at));
                }

                Ok(token)
            }
        }
    }
}
