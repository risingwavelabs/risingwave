use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::*;

use crate::{assert_not_has_tables, drop_tables};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(manager, UserDefaultPrivilege);
        manager
            .create_table(
                Table::create()
                    .table(UserDefaultPrivilege::Table)
                    .col(pk_auto(UserDefaultPrivilege::Id))
                    .col(integer(UserDefaultPrivilege::DatabaseId))
                    .col(integer_null(UserDefaultPrivilege::SchemaId))
                    .col(string(UserDefaultPrivilege::ObjectType))
                    .col(integer(UserDefaultPrivilege::UserId))
                    .col(integer(UserDefaultPrivilege::Grantee))
                    .col(integer(UserDefaultPrivilege::GrantedBy))
                    .col(string(UserDefaultPrivilege::Action))
                    .col(boolean(UserDefaultPrivilege::WithGrantOption))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_user_default_privilege_database_id")
                            .from(
                                UserDefaultPrivilege::Table,
                                UserDefaultPrivilege::DatabaseId,
                            )
                            .to(Database::Table, Database::DatabaseId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_user_default_privilege_schema_id")
                            .from(UserDefaultPrivilege::Table, UserDefaultPrivilege::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_user_default_privilege_user_id")
                            .from(UserDefaultPrivilege::Table, UserDefaultPrivilege::UserId)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_user_default_privilege_grantee_user_id")
                            .from(UserDefaultPrivilege::Table, UserDefaultPrivilege::Grantee)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_user_default_privilege_granted_user_id")
                            .from(UserDefaultPrivilege::Table, UserDefaultPrivilege::GrantedBy)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, UserDefaultPrivilege);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum UserDefaultPrivilege {
    Table,
    Id,
    DatabaseId,
    SchemaId,
    ObjectType,
    UserId,
    Grantee,
    GrantedBy,
    Action,
    WithGrantOption,
}

#[derive(DeriveIden)]
enum Database {
    Table,
    DatabaseId,
}

#[derive(DeriveIden)]
enum Schema {
    Table,
    SchemaId,
}

#[derive(DeriveIden)]
enum User {
    Table,
    UserId,
}
