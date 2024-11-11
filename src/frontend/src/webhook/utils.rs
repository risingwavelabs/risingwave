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

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use risingwave_common::row::OwnedRow;
use risingwave_pb::expr::ExprNode;
use serde_json::json;
use thiserror_ext::AsReport;

use crate::expr::ExprImpl;

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

pub async fn verify_signature(
    secret: &[u8],
    payload: &[u8],
    signature_expr: ExprNode,
    signature: &[u8],
) -> Result<bool> {
    let row = OwnedRow::new(vec![Some(secret.into()), Some(payload.into())]);
    println!("WKXLOG signature_expr: {:?}", signature_expr);

    let signature_expr_impl = ExprImpl::from_expr_proto(&signature_expr)
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    println!("WKXLOG signature_expr_impl: {:?}", signature_expr_impl);

    let result = signature_expr_impl
        .eval_row(&row)
        .await
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?
        .unwrap();
    let computed_signature = result.as_bytea();
    Ok(**computed_signature == *signature)
}
