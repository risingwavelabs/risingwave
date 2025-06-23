use sea_orm_migration::prelude::{Index as MigrationIndex, Table as MigrationTable, *};

use crate::sea_orm::{DatabaseBackend, DbBackend, Statement};
use crate::utils::ColumnDefExt;
use crate::{assert_not_has_tables, drop_tables};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. check if the table exists.
        assert_not_has_tables!(
            manager,
            Cluster,
            Worker,
            WorkerProperty,
            User,
            UserPrivilege,
            Database,
            Schema,
            StreamingJob,
            Fragment,
            Actor,
            ActorDispatcher,
            Table,
            Source,
            Sink,
            Connection,
            View,
            Index,
            Function,
            Object,
            ObjectDependency,
            SystemParameter,
            CatalogVersion
        );

        // In Mysql, The CHAR, VARCHAR and TEXT types are encoded in utf8_general_ci by default, which is not case sensitive but
        // required in risingwave. Here we need to change the database collate to utf8mb4_bin.
        if manager.get_database_backend() == DbBackend::MySql {
            manager
                .get_connection()
                .execute_unprepared("ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
                .await
                .expect("failed to set database collate");
        }

        // 2. create tables.
        manager
            .create_table(
                MigrationTable::create()
                    .table(Cluster::Table)
                    .col(
                        ColumnDef::new(Cluster::ClusterId)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Cluster::CreatedAt)
                            .date_time()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Worker::Table)
                    .col(
                        ColumnDef::new(Worker::WorkerId)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Worker::WorkerType).string().not_null())
                    .col(ColumnDef::new(Worker::Host).string().not_null())
                    .col(ColumnDef::new(Worker::Port).integer().not_null())
                    .col(ColumnDef::new(Worker::Status).string().not_null())
                    .col(ColumnDef::new(Worker::TransactionId).integer())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(WorkerProperty::Table)
                    .col(
                        ColumnDef::new(WorkerProperty::WorkerId)
                            .integer()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(WorkerProperty::ParallelUnitIds)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(WorkerProperty::IsStreaming)
                            .boolean()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(WorkerProperty::IsServing)
                            .boolean()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(WorkerProperty::IsUnschedulable)
                            .boolean()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_worker_property_worker_id")
                            .from(WorkerProperty::Table, WorkerProperty::WorkerId)
                            .to(Worker::Table, Worker::WorkerId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(User::Table)
                    .col(
                        ColumnDef::new(User::UserId)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(ColumnDef::new(User::Name).string().unique_key().not_null())
                    .col(ColumnDef::new(User::IsSuper).boolean().not_null())
                    .col(ColumnDef::new(User::CanCreateDb).boolean().not_null())
                    .col(ColumnDef::new(User::CanCreateUser).boolean().not_null())
                    .col(ColumnDef::new(User::CanLogin).boolean().not_null())
                    .col(ColumnDef::new(User::AuthInfo).rw_binary(manager))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Object::Table)
                    .col(
                        ColumnDef::new(Object::Oid)
                            .integer()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Object::ObjType).string().not_null())
                    .col(ColumnDef::new(Object::OwnerId).integer().not_null())
                    .col(ColumnDef::new(Object::SchemaId).integer())
                    .col(ColumnDef::new(Object::DatabaseId).integer())
                    .col(
                        ColumnDef::new(Object::InitializedAt)
                            .date_time()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Object::CreatedAt)
                            .date_time()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .col(ColumnDef::new(Object::InitializedAtClusterVersion).string())
                    .col(ColumnDef::new(Object::CreatedAtClusterVersion).string())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_object_owner_id")
                            .from(Object::Table, Object::OwnerId)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_object_database_id")
                            .from(Object::Table, Object::DatabaseId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_object_schema_id")
                            .from(Object::Table, Object::SchemaId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(UserPrivilege::Table)
                    .col(
                        ColumnDef::new(UserPrivilege::Id)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(ColumnDef::new(UserPrivilege::DependentId).integer())
                    .col(ColumnDef::new(UserPrivilege::UserId).integer().not_null())
                    .col(ColumnDef::new(UserPrivilege::Oid).integer().not_null())
                    .col(
                        ColumnDef::new(UserPrivilege::GrantedBy)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(UserPrivilege::Action).string().not_null())
                    .col(
                        ColumnDef::new(UserPrivilege::WithGrantOption)
                            .boolean()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_user_privilege_dependent_id")
                            .from(UserPrivilege::Table, UserPrivilege::DependentId)
                            .to(UserPrivilege::Table, UserPrivilege::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_user_privilege_user_id")
                            .from(UserPrivilege::Table, UserPrivilege::UserId)
                            .to(User::Table, User::UserId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_user_privilege_granted_by")
                            .from(UserPrivilege::Table, UserPrivilege::GrantedBy)
                            .to(User::Table, User::UserId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_user_privilege_oid")
                            .from(UserPrivilege::Table, UserPrivilege::Oid)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(ObjectDependency::Table)
                    .col(
                        ColumnDef::new(ObjectDependency::Id)
                            .integer()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ObjectDependency::Oid).integer().not_null())
                    .col(
                        ColumnDef::new(ObjectDependency::UsedBy)
                            .integer()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_object_dependency_oid")
                            .from(ObjectDependency::Table, ObjectDependency::Oid)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_object_dependency_used_by")
                            .from(ObjectDependency::Table, ObjectDependency::UsedBy)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Database::Table)
                    .col(ColumnDef::new(Database::DatabaseId).integer().primary_key())
                    .col(
                        ColumnDef::new(Database::Name)
                            .string()
                            .unique_key()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_database_object_id")
                            .from(Database::Table, Database::DatabaseId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Schema::Table)
                    .col(ColumnDef::new(Schema::SchemaId).integer().primary_key())
                    .col(ColumnDef::new(Schema::Name).string().not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_schema_object_id")
                            .from(Schema::Table, Schema::SchemaId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(StreamingJob::Table)
                    .col(ColumnDef::new(StreamingJob::JobId).integer().primary_key())
                    .col(ColumnDef::new(StreamingJob::JobStatus).string().not_null())
                    .col(ColumnDef::new(StreamingJob::CreateType).string().not_null())
                    .col(ColumnDef::new(StreamingJob::Timezone).string())
                    .col(
                        ColumnDef::new(StreamingJob::Parallelism)
                            .json_binary()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_streaming_job_object_id")
                            .from(StreamingJob::Table, StreamingJob::JobId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Fragment::Table)
                    .col(
                        ColumnDef::new(Fragment::FragmentId)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(ColumnDef::new(Fragment::JobId).integer().not_null())
                    .col(
                        ColumnDef::new(Fragment::FragmentTypeMask)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Fragment::DistributionType)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Fragment::StreamNode)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Fragment::VnodeMapping)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Fragment::StateTableIds).json_binary())
                    .col(ColumnDef::new(Fragment::UpstreamFragmentId).json_binary())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_fragment_table_id")
                            .from(Fragment::Table, Fragment::JobId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Actor::Table)
                    .col(
                        ColumnDef::new(Actor::ActorId)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(ColumnDef::new(Actor::FragmentId).integer().not_null())
                    .col(ColumnDef::new(Actor::Status).string().not_null())
                    .col(ColumnDef::new(Actor::Splits).rw_binary(manager))
                    .col(ColumnDef::new(Actor::ParallelUnitId).integer().not_null())
                    .col(ColumnDef::new(Actor::WorkerId).integer().not_null())
                    .col(ColumnDef::new(Actor::UpstreamActorIds).json_binary())
                    .col(ColumnDef::new(Actor::VnodeBitmap).rw_binary(manager))
                    .col(
                        ColumnDef::new(Actor::ExprContext)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_actor_fragment_id")
                            .from(Actor::Table, Actor::FragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(ActorDispatcher::Table)
                    .col(
                        ColumnDef::new(ActorDispatcher::Id)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(
                        ColumnDef::new(ActorDispatcher::ActorId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ActorDispatcher::DispatcherType)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ActorDispatcher::DistKeyIndices)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ActorDispatcher::OutputIndices)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ActorDispatcher::HashMapping).rw_binary(manager))
                    .col(
                        ColumnDef::new(ActorDispatcher::DispatcherId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ActorDispatcher::DownstreamActorIds)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ActorDispatcher::DownstreamTableName).string())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_actor_dispatcher_actor_id")
                            .from(ActorDispatcher::Table, ActorDispatcher::ActorId)
                            .to(Actor::Table, Actor::ActorId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_actor_dispatcher_dispatcher_id")
                            .from(ActorDispatcher::Table, ActorDispatcher::DispatcherId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Connection::Table)
                    .col(
                        ColumnDef::new(Connection::ConnectionId)
                            .integer()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Connection::Name).string().not_null())
                    .col(
                        ColumnDef::new(Connection::Info)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_connection_object_id")
                            .from(Connection::Table, Connection::ConnectionId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Source::Table)
                    .col(ColumnDef::new(Source::SourceId).integer().primary_key())
                    .col(ColumnDef::new(Source::Name).string().not_null())
                    .col(ColumnDef::new(Source::RowIdIndex).integer())
                    .col(
                        ColumnDef::new(Source::Columns)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Source::PkColumnIds).json_binary().not_null())
                    .col(
                        ColumnDef::new(Source::WithProperties)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Source::Definition)
                            .rw_long_text(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Source::SourceInfo).rw_binary(manager))
                    .col(
                        ColumnDef::new(Source::WatermarkDescs)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Source::OptionalAssociatedTableId).integer())
                    .col(ColumnDef::new(Source::ConnectionId).integer())
                    .col(ColumnDef::new(Source::Version).big_integer().not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_object_id")
                            .from(Source::Table, Source::SourceId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_connection_id")
                            .from(Source::Table, Source::ConnectionId)
                            .to(Connection::Table, Connection::ConnectionId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_optional_associated_table_id")
                            .from(Source::Table, Source::OptionalAssociatedTableId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Table::Table)
                    .col(ColumnDef::new(Table::TableId).integer().primary_key())
                    .col(ColumnDef::new(Table::Name).string().not_null())
                    .col(ColumnDef::new(Table::OptionalAssociatedSourceId).integer())
                    .col(ColumnDef::new(Table::TableType).string().not_null())
                    .col(ColumnDef::new(Table::BelongsToJobId).integer())
                    .col(ColumnDef::new(Table::Columns).rw_binary(manager).not_null())
                    .col(ColumnDef::new(Table::Pk).rw_binary(manager).not_null())
                    .col(
                        ColumnDef::new(Table::DistributionKey)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Table::StreamKey).json_binary().not_null())
                    .col(ColumnDef::new(Table::AppendOnly).boolean().not_null())
                    .col(ColumnDef::new(Table::FragmentId).integer())
                    .col(ColumnDef::new(Table::VnodeColIndex).integer())
                    .col(ColumnDef::new(Table::RowIdIndex).integer())
                    .col(ColumnDef::new(Table::ValueIndices).json_binary().not_null())
                    .col(
                        ColumnDef::new(Table::Definition)
                            .rw_long_text(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Table::HandlePkConflictBehavior)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Table::ReadPrefixLenHint)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Table::WatermarkIndices)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Table::DistKeyInPk).json_binary().not_null())
                    .col(ColumnDef::new(Table::DmlFragmentId).integer())
                    .col(ColumnDef::new(Table::Cardinality).rw_binary(manager))
                    .col(
                        ColumnDef::new(Table::CleanedByWatermark)
                            .boolean()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Table::Description).string())
                    .col(ColumnDef::new(Table::Version).rw_binary(manager))
                    .col(ColumnDef::new(Table::RetentionSeconds).integer())
                    .col(
                        ColumnDef::new(Table::IncomingSinks)
                            .json_binary()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_object_id")
                            .from(Table::Table, Table::TableId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_belongs_to_job_id")
                            .from(Table::Table, Table::BelongsToJobId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_fragment_id")
                            .from(Table::Table, Table::FragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_dml_fragment_id")
                            .from(Table::Table, Table::DmlFragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_optional_associated_source_id")
                            .from(Table::Table, Table::OptionalAssociatedSourceId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Sink::Table)
                    .col(ColumnDef::new(Sink::SinkId).integer().primary_key())
                    .col(ColumnDef::new(Sink::Name).string().not_null())
                    .col(ColumnDef::new(Sink::Columns).rw_binary(manager).not_null())
                    .col(ColumnDef::new(Sink::PlanPk).rw_binary(manager).not_null())
                    .col(
                        ColumnDef::new(Sink::DistributionKey)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Sink::DownstreamPk).json_binary().not_null())
                    .col(ColumnDef::new(Sink::SinkType).string().not_null())
                    .col(ColumnDef::new(Sink::Properties).json_binary().not_null())
                    .col(
                        ColumnDef::new(Sink::Definition)
                            .rw_long_text(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Sink::ConnectionId).integer())
                    .col(ColumnDef::new(Sink::DbName).string().not_null())
                    .col(ColumnDef::new(Sink::SinkFromName).string().not_null())
                    .col(ColumnDef::new(Sink::SinkFormatDesc).rw_binary(manager))
                    .col(ColumnDef::new(Sink::TargetTable).integer())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_sink_object_id")
                            .from(Sink::Table, Sink::SinkId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_sink_connection_id")
                            .from(Sink::Table, Sink::ConnectionId)
                            .to(Connection::Table, Connection::ConnectionId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_sink_target_table_id")
                            .from(Sink::Table, Sink::TargetTable)
                            .to(Table::Table, Table::TableId)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(View::Table)
                    .col(ColumnDef::new(View::ViewId).integer().primary_key())
                    .col(ColumnDef::new(View::Name).string().not_null())
                    .col(ColumnDef::new(View::Properties).json_binary().not_null())
                    .col(
                        ColumnDef::new(View::Definition)
                            .rw_long_text(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(View::Columns).rw_binary(manager).not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_view_object_id")
                            .from(View::Table, View::ViewId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Index::Table)
                    .col(ColumnDef::new(Index::IndexId).integer().primary_key())
                    .col(ColumnDef::new(Index::Name).string().not_null())
                    .col(ColumnDef::new(Index::IndexTableId).integer().not_null())
                    .col(ColumnDef::new(Index::PrimaryTableId).integer().not_null())
                    .col(
                        ColumnDef::new(Index::IndexItems)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Index::IndexColumnsLen).integer().not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_index_object_id")
                            .from(Index::Table, Index::IndexId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_index_index_table_id")
                            .from(Index::Table, Index::IndexTableId)
                            .to(Table::Table, Table::TableId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_index_primary_table_id")
                            .from(Index::Table, Index::PrimaryTableId)
                            .to(Table::Table, Table::TableId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Function::Table)
                    .col(ColumnDef::new(Function::FunctionId).integer().primary_key())
                    .col(ColumnDef::new(Function::Name).string().not_null())
                    .col(ColumnDef::new(Function::ArgNames).string().not_null())
                    .col(
                        ColumnDef::new(Function::ArgTypes)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Function::ReturnType)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(ColumnDef::new(Function::Language).string().not_null())
                    .col(ColumnDef::new(Function::Link).string())
                    .col(ColumnDef::new(Function::Identifier).string())
                    .col(ColumnDef::new(Function::Body).rw_long_text(manager))
                    // XXX: should this be binary type instead?
                    .col(ColumnDef::new(Function::CompressedBinary).rw_long_text(manager))
                    .col(ColumnDef::new(Function::Kind).string().not_null())
                    .col(
                        ColumnDef::new(Function::AlwaysRetryOnNetworkError)
                            .boolean()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_function_object_id")
                            .from(Function::Table, Function::FunctionId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(SystemParameter::Table)
                    .col(
                        ColumnDef::new(SystemParameter::Name)
                            .string()
                            .primary_key()
                            .not_null(),
                    )
                    .col(ColumnDef::new(SystemParameter::Value).string().not_null())
                    .col(
                        ColumnDef::new(SystemParameter::IsMutable)
                            .boolean()
                            .not_null(),
                    )
                    .col(ColumnDef::new(SystemParameter::Description).string())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                crate::Table::create()
                    .table(CatalogVersion::Table)
                    .col(
                        ColumnDef::new(CatalogVersion::Name)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(CatalogVersion::Version)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        // 3. create indexes.
        manager
            .create_index(
                MigrationIndex::create()
                    .table(Worker::Table)
                    .name("idx_worker_host_port")
                    .unique()
                    .col(Worker::Host)
                    .col(Worker::Port)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                MigrationIndex::create()
                    .table(UserPrivilege::Table)
                    .name("idx_user_privilege_item")
                    .unique()
                    .col(UserPrivilege::UserId)
                    .col(UserPrivilege::Oid)
                    .col(UserPrivilege::Action)
                    .col(UserPrivilege::GrantedBy)
                    .to_owned(),
            )
            .await?;

        // 4. initialize data.
        let insert_cluster_id = Query::insert()
            .into_table(Cluster::Table)
            .columns([Cluster::ClusterId])
            .values_panic([uuid::Uuid::new_v4().into()])
            .to_owned();
        let insert_sys_users = Query::insert()
            .into_table(User::Table)
            .columns([
                User::UserId,
                User::Name,
                User::IsSuper,
                User::CanCreateUser,
                User::CanCreateDb,
                User::CanLogin,
            ])
            .values_panic([
                1.into(),
                "root".into(),
                true.into(),
                true.into(),
                true.into(),
                true.into(),
            ])
            .values_panic([
                2.into(),
                "postgres".into(),
                true.into(),
                true.into(),
                true.into(),
                true.into(),
            ])
            .values_panic([
                3.into(),
                "rwadmin".into(),
                true.into(),
                true.into(),
                true.into(),
                true.into(),
            ])
            .to_owned();

        // Since User table is newly created, we assume that the initial user id of `root` is 1 and `postgres` is 2.
        let insert_objects = Query::insert()
            .into_table(Object::Table)
            .columns([
                Object::Oid,
                Object::ObjType,
                Object::OwnerId,
                Object::DatabaseId,
            ])
            .values_panic([1.into(), "DATABASE".into(), 1.into(), None::<i32>.into()])
            .values_panic([2.into(), "SCHEMA".into(), 1.into(), 1.into()]) // public
            .values_panic([3.into(), "SCHEMA".into(), 1.into(), 1.into()]) // pg_catalog
            .values_panic([4.into(), "SCHEMA".into(), 1.into(), 1.into()]) // information_schema
            .values_panic([5.into(), "SCHEMA".into(), 1.into(), 1.into()]) // rw_catalog
            .to_owned();

        let insert_sys_database = Query::insert()
            .into_table(Database::Table)
            .columns([Database::DatabaseId, Database::Name])
            .values_panic([1.into(), "dev".into()])
            .to_owned();
        let insert_sys_schemas = Query::insert()
            .into_table(Schema::Table)
            .columns([Schema::SchemaId, Schema::Name])
            .values_panic([2.into(), "public".into()])
            .values_panic([3.into(), "pg_catalog".into()])
            .values_panic([4.into(), "information_schema".into()])
            .values_panic([5.into(), "rw_catalog".into()])
            .to_owned();

        manager.exec_stmt(insert_cluster_id).await?;
        manager.exec_stmt(insert_sys_users).await?;
        manager.exec_stmt(insert_objects).await?;
        manager.exec_stmt(insert_sys_database).await?;
        manager.exec_stmt(insert_sys_schemas).await?;

        // Rest auto increment offset
        match manager.get_database_backend() {
            DbBackend::MySql => {
                manager
                    .get_connection()
                    .execute(Statement::from_string(
                        DatabaseBackend::MySql,
                        "ALTER TABLE object AUTO_INCREMENT = 6",
                    ))
                    .await?;
                manager
                    .get_connection()
                    .execute(Statement::from_string(
                        DatabaseBackend::MySql,
                        "ALTER TABLE user AUTO_INCREMENT = 3",
                    ))
                    .await?;
            }
            DbBackend::Postgres => {
                manager
                    .get_connection()
                    .execute(Statement::from_string(
                        DatabaseBackend::Postgres,
                        "SELECT setval('object_oid_seq', 5)",
                    ))
                    .await?;
                manager
                    .get_connection()
                    .execute(Statement::from_string(
                        DatabaseBackend::Postgres,
                        "SELECT setval('user_user_id_seq', 2)",
                    ))
                    .await?;
            }
            DbBackend::Sqlite => {}
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // drop tables cascade.
        drop_tables!(
            manager,
            Cluster,
            WorkerProperty,
            Worker,
            UserPrivilege,
            Database,
            Schema,
            StreamingJob,
            ActorDispatcher,
            Actor,
            Sink,
            Index,
            Table,
            Fragment,
            Source,
            Connection,
            View,
            Function,
            ObjectDependency,
            Object,
            User,
            SystemParameter,
            CatalogVersion
        );
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Cluster {
    Table,
    ClusterId,
    CreatedAt,
}

#[derive(DeriveIden)]
enum Worker {
    Table,
    WorkerId,
    WorkerType,
    Host,
    Port,
    TransactionId,
    Status,
}

#[derive(DeriveIden)]
enum WorkerProperty {
    Table,
    WorkerId,
    ParallelUnitIds,
    IsStreaming,
    IsServing,
    IsUnschedulable,
}

#[derive(DeriveIden)]
enum User {
    Table,
    UserId,
    Name,
    IsSuper,
    CanCreateDb,
    CanCreateUser,
    CanLogin,
    AuthInfo,
}

#[derive(DeriveIden)]
enum UserPrivilege {
    Table,
    Id,
    DependentId,
    UserId,
    Oid,
    GrantedBy,
    Action,
    WithGrantOption,
}

#[derive(DeriveIden)]
enum Database {
    Table,
    DatabaseId,
    Name,
}

#[derive(DeriveIden)]
enum Schema {
    Table,
    SchemaId,
    Name,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    JobId,
    FragmentTypeMask,
    DistributionType,
    StreamNode,
    VnodeMapping,
    StateTableIds,
    UpstreamFragmentId,
}

#[derive(DeriveIden)]
enum Actor {
    Table,
    ActorId,
    FragmentId,
    Status,
    Splits,
    ParallelUnitId,
    WorkerId,
    UpstreamActorIds,
    VnodeBitmap,
    ExprContext,
}

#[derive(DeriveIden)]
enum ActorDispatcher {
    Table,
    Id,
    ActorId,
    DispatcherType,
    DistKeyIndices,
    OutputIndices,
    HashMapping,
    DispatcherId,
    DownstreamActorIds,
    DownstreamTableName,
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    JobId,
    JobStatus,
    Timezone,
    CreateType,
    Parallelism,
}

#[derive(DeriveIden)]
#[allow(clippy::enum_variant_names)]
enum Table {
    Table,
    TableId,
    Name,
    OptionalAssociatedSourceId,
    TableType,
    BelongsToJobId,
    Columns,
    Pk,
    DistributionKey,
    StreamKey,
    AppendOnly,
    FragmentId,
    VnodeColIndex,
    RowIdIndex,
    ValueIndices,
    Definition,
    HandlePkConflictBehavior,
    ReadPrefixLenHint,
    WatermarkIndices,
    DistKeyInPk,
    DmlFragmentId,
    Cardinality,
    CleanedByWatermark,
    Description,
    Version,
    RetentionSeconds,
    IncomingSinks,
}

#[derive(DeriveIden)]
enum Source {
    Table,
    SourceId,
    Name,
    RowIdIndex,
    Columns,
    PkColumnIds,
    WithProperties,
    Definition,
    SourceInfo,
    WatermarkDescs,
    OptionalAssociatedTableId,
    ConnectionId,
    Version,
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    SinkId,
    Name,
    Columns,
    PlanPk,
    DistributionKey,
    DownstreamPk,
    SinkType,
    Properties,
    Definition,
    ConnectionId,
    DbName,
    SinkFromName,
    SinkFormatDesc,
    TargetTable,
}

#[derive(DeriveIden)]
enum Connection {
    Table,
    ConnectionId,
    Name,
    Info,
}

#[derive(DeriveIden)]
enum View {
    Table,
    ViewId,
    Name,
    Properties,
    Definition,
    Columns,
}

#[derive(DeriveIden)]
enum Index {
    Table,
    IndexId,
    Name,
    IndexTableId,
    PrimaryTableId,
    IndexItems,
    IndexColumnsLen,
}

#[derive(DeriveIden)]
enum Function {
    Table,
    FunctionId,
    Name,
    ArgNames,
    ArgTypes,
    ReturnType,
    Language,
    Link,
    Identifier,
    Body,
    CompressedBinary,
    Kind,
    AlwaysRetryOnNetworkError,
}

#[derive(DeriveIden)]
pub(crate) enum Object {
    Table,
    Oid,
    ObjType,
    OwnerId,
    SchemaId,
    DatabaseId,
    InitializedAt,
    CreatedAt,
    InitializedAtClusterVersion,
    CreatedAtClusterVersion,
}

#[derive(DeriveIden)]
enum ObjectDependency {
    Table,
    Id,
    Oid,
    UsedBy,
}

#[derive(DeriveIden)]
enum SystemParameter {
    Table,
    Name,
    Value,
    IsMutable,
    Description,
}

#[derive(DeriveIden)]
enum CatalogVersion {
    Table,
    Name,
    Version,
}
