use sea_orm_migration::prelude::*;

use crate::SubQueryStatement::SelectStatement;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .add_column(ColumnDef::new(StreamingJob::ExprContext).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        fulfill_streaming_job_expr_context(manager).await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Actor::Table)
                    .drop_column(Actor::ExprContext)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        unimplemented!()
    }
}

// Fulfill the StreamingJobExprContext table with data from the Actor and ActorDispatcher tables
async fn fulfill_streaming_job_expr_context(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let stmt = Query::update()
        .table(StreamingJob::Table)
        .value(
            StreamingJob::ExprContext,
            SimpleExpr::SubQuery(
                None,
                Box::new(SelectStatement(
                    Query::select()
                        .distinct()
                        .column((Actor::Table, Actor::ExprContext))
                        .from(Actor::Table)
                        .join(
                            JoinType::InnerJoin,
                            Fragment::Table,
                            Expr::col((Actor::Table, Actor::FragmentId))
                                .equals((Fragment::Table, Fragment::FragmentId)),
                        )
                        .and_where(
                            Expr::col((StreamingJob::Table, StreamingJob::JobId))
                                .equals((Fragment::Table, Fragment::JobId)),
                        )
                        .to_owned(),
                )),
            ),
        )
        .and_where(Expr::col((StreamingJob::Table, StreamingJob::ExprContext)).is_null())
        .to_owned();

    manager.exec_stmt(stmt).await?;

    Ok(())
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    JobId,
    ExprContext,
}

#[derive(DeriveIden)]
enum Actor {
    Table,
    FragmentId,
    ExprContext,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    JobId,
}
