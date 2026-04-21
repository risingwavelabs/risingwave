use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(UserRoleMembership::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(UserRoleMembership::Id)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(
                        ColumnDef::new(UserRoleMembership::RoleId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UserRoleMembership::MemberId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UserRoleMembership::GrantedBy)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UserRoleMembership::AdminOption)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(UserRoleMembership::InheritOption)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(UserRoleMembership::SetOption)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("FK_user_role_membership_role_id")
                            .from(UserRoleMembership::Table, UserRoleMembership::RoleId)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("FK_user_role_membership_member_id")
                            .from(UserRoleMembership::Table, UserRoleMembership::MemberId)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("FK_user_role_membership_granted_by")
                            .from(UserRoleMembership::Table, UserRoleMembership::GrantedBy)
                            .to(User::Table, User::UserId)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_user_role_membership_item")
                    .table(UserRoleMembership::Table)
                    .unique()
                    .col(UserRoleMembership::RoleId)
                    .col(UserRoleMembership::MemberId)
                    .col(UserRoleMembership::GrantedBy)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_user_role_membership_item")
                    .table(UserRoleMembership::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().table(UserRoleMembership::Table).to_owned())
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum UserRoleMembership {
    Table,
    Id,
    RoleId,
    MemberId,
    GrantedBy,
    AdminOption,
    InheritOption,
    SetOption,
}

#[derive(DeriveIden)]
enum User {
    #[sea_orm(iden = "user")]
    Table,
    UserId,
}
