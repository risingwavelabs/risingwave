use risingwave_common::rate_limit::RateLimit;
use risingwave_meta_model::RateLimit as RateLimitModel;
use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Add temp column.
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(Source::RateLimitTemp)
                            .rw_binary(manager)
                            .not_null()
                            .default(RateLimitModel::default()),
                    )
                    .to_owned(),
            )
            .await?;

        // 2. Migrate old column to the temp column.
        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();

        let (sql, values) = Query::select()
            .columns([Source::SourceId, Source::RateLimit])
            .from(Source::Table)
            .and_where(Expr::col(Source::RateLimit).is_not_null())
            .to_owned()
            .build_any(&*database_backend.get_query_builder());
        let stmt = Statement::from_sql_and_values(database_backend, sql, values);

        for OldSourceRateLimit {
            source_id,
            rate_limit,
        } in OldSourceRateLimit::find_by_statement(stmt)
            .all(connection)
            .await?
        {
            manager
                .exec_stmt(
                    Query::update()
                        .table(Source::Table)
                        .value(
                            Source::RateLimitTemp,
                            Expr::value(RateLimitModel::from(
                                &RateLimit::from(rate_limit).to_protobuf(),
                            )),
                        )
                        .and_where(Expr::col(Source::SourceId).eq(source_id))
                        .to_owned(),
                )
                .await?;
        }

        // 3. Drop old column.
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .drop_column(Source::RateLimit)
                    .to_owned(),
            )
            .await?;

        // 4. Rename temp column.
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .rename_column(Source::RateLimitTemp, Source::RateLimit)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Add temp column.
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .add_column_if_not_exists(ColumnDef::new(Source::RateLimitTemp).integer())
                    .to_owned(),
            )
            .await?;

        // 2. Migrate old column to the temp column.
        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();

        let (sql, values) = Query::select()
            .columns([Source::SourceId, Source::RateLimit])
            .from(Source::Table)
            .and_where(
                Expr::col(Source::RateLimit)
                    .ne(RateLimitModel::from(&RateLimit::Unlimited.to_protobuf())),
            )
            .to_owned()
            .build_any(&*database_backend.get_query_builder());
        let stmt = Statement::from_sql_and_values(database_backend, sql, values);

        for NewSourceRateLimit {
            source_id,
            rate_limit,
        } in NewSourceRateLimit::find_by_statement(stmt)
            .all(connection)
            .await?
        {
            manager
                .exec_stmt(
                    Query::update()
                        .table(Source::Table)
                        .value(
                            Source::RateLimitTemp,
                            Expr::value(
                                match RateLimit::from_protobuf(&rate_limit.to_protobuf()) {
                                    RateLimit::Unlimited => None,
                                    RateLimit::Fixed(rate) => Some(rate.get() as _),
                                    RateLimit::Pause => Some(0),
                                },
                            ),
                        )
                        .and_where(Expr::col(Source::SourceId).eq(source_id))
                        .to_owned(),
                )
                .await?;
        }

        // 3. Drop old column.
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .drop_column(Source::RateLimit)
                    .to_owned(),
            )
            .await?;

        // 4. Rename temp column.
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .rename_column(Source::RateLimitTemp, Source::RateLimit)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Source {
    Table,
    SourceId,
    RateLimit,
    RateLimitTemp,
}

#[derive(Debug, FromQueryResult)]
pub struct OldSourceRateLimit {
    source_id: i32,
    rate_limit: Option<i32>,
}

#[derive(Debug, FromQueryResult)]
pub struct NewSourceRateLimit {
    source_id: i32,
    rate_limit: RateLimitModel,
}
