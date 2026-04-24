// Copyright 2024 RisingWave Labs
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

use anyhow::anyhow;
use axum::Json;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use risingwave_common::row::OwnedRow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::JsonbVal;
use risingwave_pb::expr::ExprNode;
use serde_json::json;
use thiserror_ext::AsReport;

use crate::expr::ExprImpl;

#[derive(Debug)]
pub struct WebhookError {
    err: anyhow::Error,
    code: StatusCode,
}

pub(crate) type Result<T> = std::result::Result<T, WebhookError>;

pub(crate) fn err(err: impl Into<anyhow::Error>, code: StatusCode) -> WebhookError {
    WebhookError {
        err: err.into(),
        code,
    }
}

impl WebhookError {
    #[cfg(test)]
    pub(crate) fn code(&self) -> StatusCode {
        self.code
    }
}

impl From<anyhow::Error> for WebhookError {
    fn from(value: anyhow::Error) -> Self {
        WebhookError {
            err: value,
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> axum::response::Response {
        let mut resp = Json(json!({
            "error": self.err.to_report_string(),
        }))
        .into_response();
        *resp.status_mut() = self.code;
        resp
    }
}

pub(crate) fn header_map_to_json(headers: &HeaderMap) -> JsonbVal {
    let mut header_map = HashMap::new();

    for (key, value) in headers {
        let key = key.as_str().to_owned();
        let value = value.to_str().unwrap_or("").to_owned();
        header_map.insert(key, value);
    }

    let json_value = json!(header_map);
    JsonbVal::from(json_value)
}

pub(crate) async fn authenticate_webhook_payload(
    headers_jsonb: JsonbVal,
    payload: &[u8],
    webhook_source_info: &risingwave_pb::catalog::WebhookSourceInfo,
) -> Result<()> {
    let is_valid = if let Some(signature_expr) = webhook_source_info.signature_expr.clone() {
        let secret = if let Some(secret_ref) = webhook_source_info.secret_ref {
            LocalSecretManager::global()
                .fill_secret(secret_ref)
                .map_err(|e| err(e, StatusCode::NOT_FOUND))?
        } else {
            String::new()
        };
        verify_signature(headers_jsonb, secret.as_str(), payload, signature_expr).await?
    } else {
        true
    };

    if !is_valid {
        return Err(err(
            anyhow!("Signature verification failed"),
            StatusCode::UNAUTHORIZED,
        ));
    }

    Ok(())
}

pub(crate) async fn verify_signature(
    headers_jsonb: JsonbVal,
    secret: &str,
    payload: &[u8],
    signature_expr: ExprNode,
) -> Result<bool> {
    let row = OwnedRow::new(vec![
        Some(headers_jsonb.into()),
        Some(secret.into()),
        Some(payload.into()),
    ]);

    let signature_expr_impl = ExprImpl::from_expr_proto(&signature_expr)
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    let result = signature_expr_impl
        .eval_row(&row)
        .await
        .map_err(|e| {
            tracing::error!(error = %e.as_report(), "Fail to validate for webhook events.");
            err(e, StatusCode::INTERNAL_SERVER_ERROR)
        })?
        .ok_or_else(|| {
            err(
                anyhow!("`SECURE_COMPARE()` failed"),
                StatusCode::BAD_REQUEST,
            )
        })?;
    Ok(*result.as_bool())
}

#[cfg(test)]
mod tests {
    use axum::http::header::HeaderName;

    use super::*;

    #[test]
    fn test_header_map_to_json_preserves_header_names() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-custom-token"),
            "abc".parse().unwrap(),
        );

        let headers_json = header_map_to_json(&headers);
        let json_value: serde_json::Value =
            serde_json::from_str(&headers_json.to_string()).unwrap();

        assert_eq!(json_value["x-custom-token"], "abc");
    }
}
