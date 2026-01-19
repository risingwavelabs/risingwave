// Copyright 2026 RisingWave Labs
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

use std::time::{SystemTime, UNIX_EPOCH};

use http::HeaderMap;
use risingwave_meta_model::{AuditDetails, audit_log};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use serde_json::json;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use crate::manager::MetaSrvEnv;

pub const AUDIT_USER_HEADER: &str = "rw-audit-user";
pub const AUDIT_DATABASE_HEADER: &str = "rw-audit-database-id";

const AUDIT_METHOD_PREFIXES: &[&str] = &[
    "Create",
    "Drop",
    "Alter",
    "Replace",
    "Update",
    "Delete",
    "Add",
    "Remove",
    "Set",
    "Unset",
    "Apply",
    "Cancel",
    "Pause",
    "Resume",
    "Recover",
    "Refresh",
    "Reset",
    "Flush",
    "Scale",
    "Migrate",
    "Split",
    "Merge",
    "Activate",
    "Deactivate",
    "Backup",
    "Restore",
];

const AUDIT_EXEMPT_PATHS: &[&str] = &[
    "/meta.AuditLogService/AddAuditLog",
    "/meta.AuditLogService/ListAuditLog",
    "/meta.EventLogService/AddEventLog",
    "/meta.EventLogService/ListEventLog",
];

#[derive(Clone, Debug)]
pub struct AuditContext {
    pub user_name: String,
    pub database_id: Option<i32>,
}

pub fn extract_audit_context(headers: &HeaderMap) -> Option<AuditContext> {
    let user_name = headers
        .get(AUDIT_USER_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_owned())?;
    let database_id = headers
        .get(AUDIT_DATABASE_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i32>().ok());
    Some(AuditContext {
        user_name,
        database_id,
    })
}

pub fn should_audit_path(path: &str) -> bool {
    let Some(method) = method_from_path(path) else {
        return false;
    };
    method_matches_prefix(method) && !AUDIT_EXEMPT_PATHS.contains(&path)
}

pub async fn write_audit_log(env: &MetaSrvEnv, ctx: AuditContext, path: &str) {
    let Ok(event_time) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return;
    };
    let details = AuditDetails::from(json!({ "rpc_path": path }));
    let audit = audit_log::ActiveModel {
        id: Default::default(),
        event_time: Set(event_time.as_millis() as i64),
        user_name: Set(ctx.user_name),
        action: Set(action_from_path(path)),
        object_type: Set(None),
        object_id: Set(None),
        object_name: Set(None),
        database_id: Set(ctx.database_id),
        details: Set(Some(details)),
    };

    if let Err(err) = audit.insert(&env.meta_store().conn).await {
        tracing::warn!(error = %err, "failed to write audit log");
    }
}

pub fn start_audit_log_cleanup(env: MetaSrvEnv) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let retention_days = env.opts.audit_log_retention_days;
    let cleanup_interval = env.opts.audit_log_cleanup_interval_sec;
    let join_handle = tokio::spawn(async move {
        if retention_days == 0 || cleanup_interval == 0 {
            tracing::info!("Audit log cleanup is disabled");
            return;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(cleanup_interval));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                _ = &mut shutdown_rx => {
                    tracing::info!("Audit log cleanup loop is stopped");
                    return;
                }
            }
            let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
                continue;
            };
            let retention_ms = (retention_days as u64).saturating_mul(24 * 60 * 60 * 1000);
            let cutoff = now.as_millis().saturating_sub(retention_ms as u128) as i64;
            if let Err(err) = audit_log::Entity::delete_many()
                .filter(audit_log::Column::EventTime.lt(cutoff))
                .exec(&env.meta_store().conn)
                .await
            {
                tracing::warn!(error = %err, "failed to cleanup audit logs");
            }
        }
    });
    (join_handle, shutdown_tx)
}

fn method_matches_prefix(method: &str) -> bool {
    AUDIT_METHOD_PREFIXES
        .iter()
        .any(|prefix| method.starts_with(prefix))
}

fn method_from_path(path: &str) -> Option<&str> {
    path.rsplit('/').next()
}

fn action_from_path(path: &str) -> String {
    method_from_path(path)
        .map(|method| method.to_owned())
        .unwrap_or_else(|| path.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ProtoSource<'a> {
        package: &'a str,
        content: &'a str,
        services: &'a [&'a str],
    }

    fn parse_package(proto: &str) -> Option<String> {
        for line in proto.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("package ") {
                return rest
                    .trim_end_matches(';')
                    .trim()
                    .split_whitespace()
                    .next()
                    .map(String::from);
            }
        }
        None
    }

    fn parse_services(proto: &str) -> std::collections::HashMap<String, Vec<String>> {
        let mut map = std::collections::HashMap::new();
        let mut current = None::<String>;
        for line in proto.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("service ") {
                let name = rest
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_end_matches('{');
                current = Some(name.to_owned());
                continue;
            }
            if line.starts_with('}') {
                current = None;
                continue;
            }
            if let Some(service) = current.as_ref() {
                if let Some(rest) = line.strip_prefix("rpc ") {
                    let method = rest.split('(').next().unwrap_or("").trim();
                    if !method.is_empty() {
                        map.entry(service.clone())
                            .or_default()
                            .push(method.to_owned());
                    }
                }
            }
        }
        map
    }

    #[test]
    fn audit_coverage_for_mutating_rpcs() {
        let sources = [
            ProtoSource {
                package: "meta",
                content: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../proto/meta.proto"
                )),
                services: &[
                    "StreamManagerService",
                    "ClusterService",
                    "SystemParamsService",
                    "SessionParamService",
                    "UserService",
                    "ScaleService",
                    "BackupService",
                    "ClusterLimitService",
                    "HostedIcebergCatalogService",
                    "ServingService",
                    "HummockManagerService",
                ],
            },
            ProtoSource {
                package: "ddl_service",
                content: include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../proto/ddl_service.proto"
                )),
                services: &["DdlService"],
            },
        ];

        for source in sources {
            let package =
                parse_package(source.content).unwrap_or_else(|| source.package.to_owned());
            let services = parse_services(source.content);
            for service in source.services {
                let Some(methods) = services.get(*service) else {
                    continue;
                };
                for method in methods {
                    if !method_matches_prefix(method) {
                        continue;
                    }
                    let path = format!("/{package}.{service}/{method}");
                    if AUDIT_EXEMPT_PATHS.contains(&path.as_str()) {
                        continue;
                    }
                    assert!(
                        should_audit_path(&path),
                        "RPC path should be audited or exempted: {}",
                        path
                    );
                }
            }
        }
    }
}
