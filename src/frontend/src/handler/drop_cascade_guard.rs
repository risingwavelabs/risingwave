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

use risingwave_pb::common::PbObjectType;
use risingwave_pb::ddl_service::PreviewDropCascadeResponse;

use crate::error::{ErrorCode, Result};
use crate::session::SessionImpl;

pub async fn guard_drop_cascade(
    session: &SessionImpl,
    statement_name: &str,
    object_type: PbObjectType,
    object_id: u32,
) -> Result<()> {
    let preview = session
        .env()
        .meta_client()
        .preview_drop_cascade(object_id, object_type)
        .await?;
    let impact = format_drop_cascade_impact(&preview);

    if !session.config().allow_drop_cascade() {
        return Err(ErrorCode::NotSupported(
            format!("{statement_name} CASCADE is disabled. {impact}"),
            "SET allow_drop_cascade = true to enable DROP ... CASCADE in the current session, or ALTER SYSTEM SET allow_drop_cascade = true to change the default."
                .to_owned(),
        )
        .into());
    }

    if preview.total_count > 1 {
        session.notice_to_user(format!("{statement_name} CASCADE {impact}"));
    }

    Ok(())
}

fn format_drop_cascade_impact(preview: &PreviewDropCascadeResponse) -> String {
    if preview.object_counts.is_empty() {
        return format!("It would remove {} object(s).", preview.total_count);
    }

    let breakdown = preview
        .object_counts
        .iter()
        .filter_map(|count| {
            let object_type = PbObjectType::try_from(count.object_type).ok()?;
            Some(format!(
                "{} {}",
                count.count,
                format_object_type_name(object_type, count.count)
            ))
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "It would remove {} object(s): {}.",
        preview.total_count, breakdown
    )
}

fn format_object_type_name(object_type: PbObjectType, count: u64) -> &'static str {
    match (object_type, count == 1) {
        (PbObjectType::Database, true) => "database",
        (PbObjectType::Database, false) => "databases",
        (PbObjectType::Schema, true) => "schema",
        (PbObjectType::Schema, false) => "schemas",
        (PbObjectType::Table, true) => "table",
        (PbObjectType::Table, false) => "tables",
        (PbObjectType::Mview, true) => "materialized view",
        (PbObjectType::Mview, false) => "materialized views",
        (PbObjectType::Source, true) => "source",
        (PbObjectType::Source, false) => "sources",
        (PbObjectType::Sink, true) => "sink",
        (PbObjectType::Sink, false) => "sinks",
        (PbObjectType::View, true) => "view",
        (PbObjectType::View, false) => "views",
        (PbObjectType::Index, true) => "index",
        (PbObjectType::Index, false) => "indexes",
        (PbObjectType::Function, true) => "function",
        (PbObjectType::Function, false) => "functions",
        (PbObjectType::Connection, true) => "connection",
        (PbObjectType::Connection, false) => "connections",
        (PbObjectType::Subscription, true) => "subscription",
        (PbObjectType::Subscription, false) => "subscriptions",
        (PbObjectType::Secret, true) => "secret",
        (PbObjectType::Secret, false) => "secrets",
        (PbObjectType::Unspecified, true) => "object",
        (PbObjectType::Unspecified, false) => "objects",
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::ddl_service::DropCascadeObjectCount;

    use super::*;

    #[test]
    fn test_format_drop_cascade_impact() {
        let preview = PreviewDropCascadeResponse {
            total_count: 4,
            object_counts: vec![
                DropCascadeObjectCount {
                    object_type: PbObjectType::Schema as i32,
                    count: 1,
                },
                DropCascadeObjectCount {
                    object_type: PbObjectType::Secret as i32,
                    count: 3,
                },
            ],
        };

        assert_eq!(
            format_drop_cascade_impact(&preview),
            "It would remove 4 object(s): 1 schema, 3 secrets."
        );
    }
}
