// Copyright 2023 RisingWave Labs
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

use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::Result;
use risingwave_pb::user::grant_privilege::{PbAction, PbObject};

use crate::binder::{BoundStatement, Relation};
use crate::catalog::RelationCatalog;
use crate::session::SessionImpl;
use crate::user::UserId;

pub struct ObjectCheckItem {
    owner: UserId,
    action: PbAction,
    object: PbObject,
}

impl ObjectCheckItem {
    pub fn new(owner: UserId, action: PbAction, object: PbObject) -> Self {
        Self {
            owner,
            action,
            object,
        }
    }
}

/// resolve privileges in `relation`
pub(crate) fn resolve_relation_privileges(
    relation: &Relation,
    action: PbAction,
    objects: &mut Vec<ObjectCheckItem>,
) {
    match relation {
        Relation::Source(source) => {
            let item = ObjectCheckItem {
                owner: source.catalog.owner,
                action,
                object: PbObject::SourceId(source.catalog.id),
            };
            objects.push(item);
        }
        Relation::BaseTable(table) => {
            let item = ObjectCheckItem {
                owner: table.table_catalog.owner,
                action,
                object: PbObject::TableId(table.table_id.table_id),
            };
            objects.push(item);
        }
        Relation::Subquery(query) => {
            if let crate::binder::BoundSetExpr::Select(select) = &query.query.body {
                if let Some(sub_relation) = &select.from {
                    resolve_relation_privileges(sub_relation, action, objects);
                }
            }
        }
        Relation::Join(join) => {
            resolve_relation_privileges(&join.left, action, objects);
            resolve_relation_privileges(&join.right, action, objects);
        }
        Relation::WindowTableFunction(table) => {
            resolve_relation_privileges(&table.input, action, objects)
        }
        _ => {}
    };
}

/// resolve privileges in `stmt`
pub(crate) fn resolve_privileges(stmt: &BoundStatement) -> Vec<ObjectCheckItem> {
    let mut objects = Vec::new();
    match stmt {
        BoundStatement::Insert(ref insert) => {
            let object = ObjectCheckItem {
                owner: insert.owner,
                action: PbAction::Insert,
                object: PbObject::TableId(insert.table_id.table_id),
            };
            objects.push(object);
            if let crate::binder::BoundSetExpr::Select(select) = &insert.source.body {
                if let Some(sub_relation) = &select.from {
                    resolve_relation_privileges(sub_relation, PbAction::Select, &mut objects);
                }
            }
        }
        BoundStatement::Delete(ref delete) => {
            let object = ObjectCheckItem {
                owner: delete.owner,
                action: PbAction::Delete,
                object: PbObject::TableId(delete.table_id.table_id),
            };
            objects.push(object);
        }
        BoundStatement::Update(ref update) => {
            let object = ObjectCheckItem {
                owner: update.owner,
                action: PbAction::Update,
                object: PbObject::TableId(update.table_id.table_id),
            };
            objects.push(object);
        }
        BoundStatement::Query(ref query) => {
            if let crate::binder::BoundSetExpr::Select(select) = &query.body {
                if let Some(sub_relation) = &select.from {
                    resolve_relation_privileges(sub_relation, PbAction::Select, &mut objects);
                }
            }
        }
    };
    objects
}

impl SessionImpl {
    /// Check whether the user of the current session has privileges in `items`.
    pub fn check_privileges(&self, items: &[ObjectCheckItem]) -> Result<()> {
        let user_reader = self.env().user_info_reader();
        let reader = user_reader.read_guard();

        if let Some(info) = reader.get_user_by_name(self.user_name()) {
            if info.is_super {
                return Ok(());
            }
            for item in items {
                if item.owner == info.id {
                    continue;
                }
                let has_privilege = info.grant_privileges.iter().any(|privilege| {
                    privilege.object.is_some()
                        && privilege.object.as_ref().unwrap() == &item.object
                        && privilege
                            .action_with_opts
                            .iter()
                            .any(|ao| ao.action == item.action as i32)
                });
                if !has_privilege {
                    return Err(PermissionDenied("Do not have the privilege".to_string()).into());
                }
            }
        } else {
            return Err(PermissionDenied("Session user is invalid".to_string()).into());
        }

        Ok(())
    }

    /// Returns `true` if the user of the current session is a super user.
    fn is_super_user(&self) -> bool {
        let reader = self.env().user_info_reader().read_guard();

        if let Some(info) = reader.get_user_by_name(self.user_name()) {
            info.is_super
        } else {
            false
        }
    }

    /// Check whether the user of the current session has the privilege to drop or alter the
    /// relation `relation` in the schema with name `schema_name`.
    ///
    /// Note that the right to drop or alter in PostgreSQL is special and not covered by the general
    /// `GRANT`s.
    ///
    /// > The right to drop an object, or to alter its definition in any way, is not treated as a
    /// > grantable privilege; it is inherent in the owner, and cannot be granted or revoked.
    /// >
    /// > Reference: <https://www.postgresql.org/docs/current/sql-grant.html>
    pub fn check_privilege_for_drop_alter(
        &self,
        schema_name: &str,
        relation: &impl RelationCatalog,
    ) -> Result<()> {
        let schema_owner = self
            .env()
            .catalog_reader()
            .read_guard()
            .get_schema_by_name(self.database(), schema_name)
            .unwrap()
            .owner();

        // https://www.postgresql.org/docs/current/sql-droptable.html
        if self.user_id() != relation.owner()
            && self.user_id() != schema_owner
            && !self.is_super_user()
        {
            return Err(PermissionDenied(
                "Only the relation owner, the schema owner, and superuser can drop a relation."
                    .to_string(),
            )
            .into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER_ID};

    use super::*;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_check_privileges() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();
        frontend.run_sql("CREATE SCHEMA schema").await.unwrap();

        let schema = catalog_reader
            .read_guard()
            .get_schema_by_name(DEFAULT_DATABASE_NAME, "schema")
            .unwrap()
            .clone();
        let check_items = vec![ObjectCheckItem::new(
            DEFAULT_SUPER_USER_ID,
            PbAction::Create,
            PbObject::SchemaId(schema.id()),
        )];
        assert!(&session.check_privileges(&check_items).is_ok());

        frontend
            .run_sql(
                "CREATE USER user WITH NOSUPERUSER PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'",
            )
            .await
            .unwrap();
        let database = DEFAULT_DATABASE_NAME.to_string();
        let user_name = "user".to_string();
        let user_id = {
            let user_reader = session.env().user_info_reader();
            user_reader
                .read_guard()
                .get_user_by_name("user")
                .unwrap()
                .id
        };
        let session = frontend.session_user_ref(database, user_name, user_id);
        assert!(&session.check_privileges(&check_items).is_err());

        frontend
            .run_sql("GRANT CREATE ON SCHEMA schema TO user")
            .await
            .unwrap();
        assert!(&session.check_privileges(&check_items).is_ok());
    }
}
