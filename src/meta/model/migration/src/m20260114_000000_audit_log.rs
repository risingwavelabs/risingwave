use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(AuditLog::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(AuditLog::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(AuditLog::EventTime).big_integer().not_null())
                    .col(ColumnDef::new(AuditLog::UserName).string().not_null())
                    .col(ColumnDef::new(AuditLog::Action).string().not_null())
                    .col(ColumnDef::new(AuditLog::ObjectType).string().null())
                    .col(ColumnDef::new(AuditLog::ObjectId).integer().null())
                    .col(ColumnDef::new(AuditLog::ObjectName).string().null())
                    .col(ColumnDef::new(AuditLog::DatabaseId).integer().null())
                    .col(ColumnDef::new(AuditLog::Details).json_binary().null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(AuditLog::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum AuditLog {
    Table,
    Id,
    EventTime,
    UserName,
    Action,
    ObjectType,
    ObjectId,
    ObjectName,
    DatabaseId,
    Details,
}
