use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add is_admin column to user table
        manager
            .alter_table(
                Table::alter()
                    .table(UserEnum::User)
                    .add_column(
                        ColumnDef::new(UserEnum::IsAdmin)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        // Set is_admin = true for the rwadmin user if it exists
        // Note: rwadmin is already a superuser by default, but we ensure consistency
        let update_stmt = Query::update()
            .table(UserEnum::User)
            .values([(UserEnum::IsAdmin, true.into())])
            .and_where(Expr::col(UserEnum::Name).eq("rwadmin"))
            .to_owned();
        
        manager.exec_stmt(update_stmt).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(UserEnum::User)
                    .drop_column(UserEnum::IsAdmin)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum UserEnum {
    #[sea_orm(iden = "user")]
    User,
    Name,
    IsAdmin,
}