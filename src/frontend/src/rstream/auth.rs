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

use anyhow::anyhow;
use axum::http::{HeaderMap, StatusCode};
use rand::Rng;

use super::types::{err, Result};
use crate::session::SESSION_MANAGER;
use crate::user::UserId;
use crate::user::user_authentication::md5_hash;

const TOKEN_PREFIX: &str = "rstream_";
const TOKEN_HEX_LEN: usize = 32;

/// User info extracted from a valid bearer token.
#[derive(Clone, Debug)]
pub struct AuthenticatedUser {
    pub user_name: String,
    pub user_id: UserId,
}

/// Generate a new token: `rstream_` + 32 random hex characters.
pub fn generate_token() -> String {
    let mut rng = rand::rng();
    let hex: String = (0..TOKEN_HEX_LEN)
        .map(|_| format!("{:x}", rng.random_range(0u8..16)))
        .collect();
    format!("{}{}", TOKEN_PREFIX, hex)
}

/// Extract the bearer token string from the `Authorization` header.
pub fn extract_bearer_token(headers: &HeaderMap) -> Result<String> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| err(anyhow!("missing Authorization header"), StatusCode::UNAUTHORIZED))?
        .to_str()
        .map_err(|_| err(anyhow!("invalid Authorization header"), StatusCode::UNAUTHORIZED))?;

    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| {
            err(
                anyhow!("Authorization header must use Bearer scheme"),
                StatusCode::UNAUTHORIZED,
            )
        })?
        .trim();

    if token.is_empty() {
        return Err(err(anyhow!("empty bearer token"), StatusCode::UNAUTHORIZED));
    }

    Ok(token.to_owned())
}

/// Authenticate a bearer token by looking up the corresponding database user
/// and verifying the password hash.
///
/// The token is used as both the username and password. The stored hash is
/// `md5(token + token)` (password=token, username=token).
pub fn authenticate_token(token: &str) -> Result<AuthenticatedUser> {
    let session_mgr = SESSION_MANAGER
        .get()
        .expect("session manager has been initialized");

    let user_reader = session_mgr.env().user_info_reader();
    let reader = user_reader.read_guard();

    let user = reader.get_user_by_name(token).ok_or_else(|| {
        err(
            anyhow!("invalid or revoked token"),
            StatusCode::UNAUTHORIZED,
        )
    })?;

    if !user.can_login {
        return Err(err(
            anyhow!("token user cannot login"),
            StatusCode::UNAUTHORIZED,
        ));
    }

    // Verify password: the token is both username and password, so stored hash
    // is md5(token + token). Compare with user.auth_info.
    let auth_info = user.auth_info.as_ref().ok_or_else(|| {
        err(
            anyhow!("token user has no password set"),
            StatusCode::UNAUTHORIZED,
        )
    })?;

    let expected_hash = md5_hash(token, token);
    if auth_info.encrypted_value != expected_hash {
        return Err(err(anyhow!("invalid token"), StatusCode::UNAUTHORIZED));
    }

    Ok(AuthenticatedUser {
        user_name: user.name.clone(),
        user_id: user.id,
    })
}

/// Check if a token name looks like an rstream token.
pub fn is_rstream_token_user(name: &str) -> bool {
    name.starts_with(TOKEN_PREFIX) && name.len() == TOKEN_PREFIX.len() + TOKEN_HEX_LEN
}

/// Read the admin secret from the `RSTREAM_ADMIN_SECRET` environment variable.
/// Returns `None` in dev mode (env var not set), meaning token management is open.
pub fn admin_secret() -> Option<String> {
    std::env::var("RSTREAM_ADMIN_SECRET").ok().filter(|s| !s.is_empty())
}

/// Verify the admin secret from query params or header.
/// If `RSTREAM_ADMIN_SECRET` is not set, access is open (dev mode).
pub fn verify_admin_secret(headers: &HeaderMap) -> Result<()> {
    let Some(expected) = admin_secret() else {
        return Ok(()); // dev mode — open access
    };

    // Check Authorization header: Bearer <admin_secret>
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.trim())
        .unwrap_or("");

    if provided == expected {
        Ok(())
    } else {
        Err(err(
            anyhow!("invalid admin secret"),
            StatusCode::UNAUTHORIZED,
        ))
    }
}
