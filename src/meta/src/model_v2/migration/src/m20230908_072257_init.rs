use sea_orm_migration::prelude::{Index as MigrationIndex, Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. check if the table exists.
        assert!(!manager.has_table(Cluster::Table.to_string()).await?);
        assert!(!manager.has_table(Worker::Table.to_string()).await?);
        assert!(!manager.has_table(WorkerProperty::Table.to_string()).await?);
        assert!(!manager.has_table(User::Table.to_string()).await?);
        assert!(!manager.has_table(UserPrivilege::Table.to_string()).await?);
        assert!(!manager.has_table(Database::Table.to_string()).await?);
        assert!(!manager.has_table(Schema::Table.to_string()).await?);
        assert!(!manager.has_table(Fragment::Table.to_string()).await?);
        assert!(!manager.has_table(Actor::Table.to_string()).await?);
        assert!(!manager.has_table(Table::Table.to_string()).await?);
        assert!(!manager.has_table(Source::Table.to_string()).await?);
        assert!(!manager.has_table(Sink::Table.to_string()).await?);
        assert!(!manager.has_table(Connection::Table.to_string()).await?);
        assert!(!manager.has_table(View::Table.to_string()).await?);
        assert!(!manager.has_table(Index::Table.to_string()).await?);
        assert!(!manager.has_table(Function::Table.to_string()).await?);
        assert!(!manager.has_table(Object::Table.to_string()).await?);
        assert!(
            !manager
                .has_table(ObjectDependency::Table.to_string())
                .await?
        );
        assert!(
            !manager
                .has_table(SystemParameter::Table.to_string())
                .await?
        );
        assert!(!manager.has_table(ElectionLeader::Table.to_string()).await?);
        assert!(!manager.has_table(ElectionMember::Table.to_string()).await?);

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
                            .timestamp()
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
                            .json()
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
                    .col(ColumnDef::new(User::Name).string().not_null())
                    .col(ColumnDef::new(User::IsSuper).boolean().not_null())
                    .col(ColumnDef::new(User::CanCreateDb).boolean().not_null())
                    .col(ColumnDef::new(User::CanCreateUser).boolean().not_null())
                    .col(ColumnDef::new(User::CanLogin).boolean().not_null())
                    .col(ColumnDef::new(User::AuthType).string())
                    .col(ColumnDef::new(User::AuthValue).string())
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
                    .col(
                        ColumnDef::new(Object::InitializedAt)
                            .timestamp()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Object::CreatedAt)
                            .timestamp()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_object_owner_id")
                            .from(Object::Table, Object::OwnerId)
                            .to(User::Table, User::UserId)
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
                    .col(ColumnDef::new(UserPrivilege::UserId).integer().not_null())
                    .col(ColumnDef::new(UserPrivilege::Oid).integer().not_null())
                    .col(
                        ColumnDef::new(UserPrivilege::GrantedBy)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(UserPrivilege::Actions).string().not_null())
                    .col(
                        ColumnDef::new(UserPrivilege::WithGrantOption)
                            .boolean()
                            .not_null(),
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
                    .col(ColumnDef::new(Schema::DatabaseId).integer().not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_schema_database_id")
                            .from(Schema::Table, Schema::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
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
                    .table(Fragment::Table)
                    .col(
                        ColumnDef::new(Fragment::FragmentId)
                            .integer()
                            .primary_key()
                            .auto_increment(),
                    )
                    .col(ColumnDef::new(Fragment::TableId).integer().not_null())
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
                    .col(ColumnDef::new(Fragment::StreamNode).json().not_null())
                    .col(ColumnDef::new(Fragment::VnodeMapping).json())
                    .col(ColumnDef::new(Fragment::StateTableIds).json())
                    .col(ColumnDef::new(Fragment::UpstreamFragmentId).json())
                    .col(ColumnDef::new(Fragment::DispatcherType).string())
                    .col(ColumnDef::new(Fragment::DistKeyIndices).json())
                    .col(ColumnDef::new(Fragment::OutputIndices).json())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_fragment_table_id")
                            .from(Fragment::Table, Fragment::TableId)
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
                    .col(ColumnDef::new(Actor::Status).string())
                    .col(ColumnDef::new(Actor::Splits).json())
                    .col(ColumnDef::new(Actor::ParallelUnitId).integer().not_null())
                    .col(ColumnDef::new(Actor::UpstreamActorIds).json())
                    .col(ColumnDef::new(Actor::Dispatchers).json())
                    .col(ColumnDef::new(Actor::VnodeBitmap).string())
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
                    .table(Connection::Table)
                    .col(
                        ColumnDef::new(Connection::ConnectionId)
                            .integer()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Connection::Name).string().not_null())
                    .col(ColumnDef::new(Connection::SchemaId).integer().not_null())
                    .col(ColumnDef::new(Connection::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(Connection::Info).json())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_connection_database_id")
                            .from(Connection::Table, Connection::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_connection_schema_id")
                            .from(Connection::Table, Connection::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
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
                    .col(ColumnDef::new(Source::SchemaId).integer().not_null())
                    .col(ColumnDef::new(Source::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(Source::RowIdIndex).string())
                    .col(ColumnDef::new(Source::Columns).json())
                    .col(ColumnDef::new(Source::PkColumnIds).json())
                    .col(ColumnDef::new(Source::Properties).json())
                    .col(ColumnDef::new(Source::Definition).string())
                    .col(ColumnDef::new(Source::SourceInfo).json())
                    .col(ColumnDef::new(Source::WatermarkDescs).json())
                    .col(ColumnDef::new(Source::OptionalAssociatedTableId).integer())
                    .col(ColumnDef::new(Source::ConnectionId).integer())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_database_id")
                            .from(Source::Table, Source::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_schema_id")
                            .from(Source::Table, Source::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
                    )
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
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(Table::Table)
                    .col(ColumnDef::new(Table::TableId).integer().primary_key())
                    .col(ColumnDef::new(Table::Name).string().not_null())
                    .col(ColumnDef::new(Table::SchemaId).integer().not_null())
                    .col(ColumnDef::new(Table::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(Table::OptionalAssociatedSourceId).integer())
                    .col(ColumnDef::new(Table::TableType).string())
                    .col(ColumnDef::new(Table::Columns).json())
                    .col(ColumnDef::new(Table::Pk).json())
                    .col(ColumnDef::new(Table::DistributionKey).json())
                    .col(ColumnDef::new(Table::AppendOnly).boolean())
                    .col(ColumnDef::new(Table::Properties).json())
                    .col(ColumnDef::new(Table::FragmentId).integer())
                    .col(ColumnDef::new(Table::VnodeColIndex).integer())
                    .col(ColumnDef::new(Table::ValueIndices).json())
                    .col(ColumnDef::new(Table::Definition).string())
                    .col(ColumnDef::new(Table::HandlePkConflictBehavior).integer())
                    .col(ColumnDef::new(Table::ReadPrefixLenHint).integer())
                    .col(ColumnDef::new(Table::WatermarkIndices).json())
                    .col(ColumnDef::new(Table::DistKeyInPk).json())
                    .col(ColumnDef::new(Table::DmlFragmentId).integer())
                    .col(ColumnDef::new(Table::Cardinality).json())
                    .col(ColumnDef::new(Table::CleanedByWatermark).boolean())
                    .col(ColumnDef::new(Table::Version).json())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_database_id")
                            .from(Table::Table, Table::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_schema_id")
                            .from(Table::Table, Table::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_view_object_id")
                            .from(Table::Table, Table::TableId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_table_fragment_id")
                            .from(Table::Table, Table::FragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
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
                            .to(Source::Table, Source::SourceId)
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
                    .col(ColumnDef::new(Sink::SchemaId).integer().not_null())
                    .col(ColumnDef::new(Sink::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(Sink::Columns).json())
                    .col(ColumnDef::new(Sink::PkColumnIds).json())
                    .col(ColumnDef::new(Sink::DistributionKey).json())
                    .col(ColumnDef::new(Sink::DownstreamPk).json())
                    .col(ColumnDef::new(Sink::SinkType).string().not_null())
                    .col(ColumnDef::new(Sink::Properties).json())
                    .col(ColumnDef::new(Sink::Definition).string().not_null())
                    .col(ColumnDef::new(Sink::ConnectionId).integer())
                    .col(ColumnDef::new(Sink::DbName).string().not_null())
                    .col(ColumnDef::new(Sink::SinkFromName).string().not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_sink_database_id")
                            .from(Sink::Table, Sink::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_sink_schema_id")
                            .from(Sink::Table, Sink::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
                    )
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
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(View::Table)
                    .col(ColumnDef::new(View::ViewId).integer().primary_key())
                    .col(ColumnDef::new(View::Name).string().not_null())
                    .col(ColumnDef::new(View::SchemaId).integer().not_null())
                    .col(ColumnDef::new(View::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(View::Properties).json())
                    .col(ColumnDef::new(View::Sql).string())
                    .col(ColumnDef::new(View::Columns).json())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_view_database_id")
                            .from(View::Table, View::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_view_schema_id")
                            .from(View::Table, View::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
                    )
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
                    .col(ColumnDef::new(Index::SchemaId).integer().not_null())
                    .col(ColumnDef::new(Index::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(Index::IndexTableId).integer().not_null())
                    .col(ColumnDef::new(Index::PrimaryTableId).integer().not_null())
                    .col(ColumnDef::new(Index::IndexItems).json())
                    .col(ColumnDef::new(Index::OriginalColumns).json())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_index_database_id")
                            .from(Index::Table, Index::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_index_schema_id")
                            .from(Index::Table, Index::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
                    )
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
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_index_primary_table_id")
                            .from(Index::Table, Index::PrimaryTableId)
                            .to(Table::Table, Table::TableId)
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
                    .col(ColumnDef::new(Function::SchemaId).integer().not_null())
                    .col(ColumnDef::new(Function::DatabaseId).integer().not_null())
                    .col(ColumnDef::new(Function::ArgTypes).json())
                    .col(ColumnDef::new(Function::ReturnType).string())
                    .col(ColumnDef::new(Function::Language).string())
                    .col(ColumnDef::new(Function::Link).string())
                    .col(ColumnDef::new(Function::Identifier).string())
                    .col(ColumnDef::new(Function::Kind).json())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_function_database_id")
                            .from(Function::Table, Function::DatabaseId)
                            .to(Database::Table, Database::DatabaseId)
                            .to_owned(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_function_schema_id")
                            .from(Function::Table, Function::SchemaId)
                            .to(Schema::Table, Schema::SchemaId)
                            .to_owned(),
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
                MigrationTable::create()
                    .table(ElectionLeader::Table)
                    .col(
                        ColumnDef::new(ElectionLeader::Service)
                            .string()
                            .primary_key()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ElectionLeader::Id).string().not_null())
                    .col(
                        ColumnDef::new(ElectionLeader::LastHeartbeat)
                            .timestamp()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                MigrationTable::create()
                    .table(ElectionMember::Table)
                    .col(ColumnDef::new(ElectionMember::Service).string().not_null())
                    .col(
                        ColumnDef::new(ElectionMember::Id)
                            .string()
                            .primary_key()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ElectionMember::LastHeartbeat)
                            .timestamp()
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
                    .table(Schema::Table)
                    .name("idx_schema_database_id_name")
                    .unique()
                    .col(Schema::DatabaseId)
                    .col(Schema::Name)
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
                User::Name,
                User::IsSuper,
                User::CanCreateUser,
                User::CanCreateDb,
                User::CanLogin,
            ])
            .values_panic([
                "root".into(),
                true.into(),
                true.into(),
                true.into(),
                true.into(),
            ])
            .values_panic([
                "postgres".into(),
                true.into(),
                true.into(),
                true.into(),
                true.into(),
            ])
            .to_owned();

        // Since User table is newly created, we assume that the initial user id of `root` is 1 and `postgres` is 2.
        let insert_objects = Query::insert()
            .into_table(Object::Table)
            .columns([Object::ObjType, Object::OwnerId])
            .values_panic(["DATABASE".into(), 1.into()])
            .values_panic(["SCHEMA".into(), 1.into()]) // public
            .values_panic(["SCHEMA".into(), 1.into()]) // pg_catalog
            .values_panic(["SCHEMA".into(), 1.into()]) // information_schema
            .values_panic(["SCHEMA".into(), 1.into()]) // rw_catalog
            .to_owned();

        // Since all tables are newly created, we assume that the initial object id of `dev` is 1 and the schemas' ids are 2, 3, 4, 5.
        let insert_sys_database = Query::insert()
            .into_table(Database::Table)
            .columns([Database::DatabaseId, Database::Name])
            .values_panic([1.into(), "dev".into()])
            .to_owned();
        let insert_sys_schemas = Query::insert()
            .into_table(Schema::Table)
            .columns([Schema::SchemaId, Schema::Name, Schema::DatabaseId])
            .values_panic([2.into(), "public".into(), 1.into()])
            .values_panic([3.into(), "pg_catalog".into(), 1.into()])
            .values_panic([4.into(), "information_schema".into(), 1.into()])
            .values_panic([5.into(), "rw_catalog".into(), 1.into()])
            .to_owned();

        manager.exec_stmt(insert_cluster_id).await?;
        manager.exec_stmt(insert_sys_users).await?;
        manager.exec_stmt(insert_objects).await?;
        manager.exec_stmt(insert_sys_database).await?;
        manager.exec_stmt(insert_sys_schemas).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        macro_rules! drop_tables {
            ($manager:expr, $( $table:ident ),+) => {
                $(
                    $manager
                        .drop_table(
                            MigrationTable::drop()
                                .table($table::Table)
                                .if_exists()
                                .cascade()
                                .to_owned(),
                        )
                        .await?;
                )+
            };
        }

        // drop tables cascade.
        drop_tables!(
            manager,
            Cluster,
            Worker,
            WorkerProperty,
            User,
            UserPrivilege,
            Database,
            Schema,
            Fragment,
            Actor,
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
            ElectionLeader,
            ElectionMember
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
    AuthType,
    AuthValue,
}

#[derive(DeriveIden)]
enum UserPrivilege {
    Table,
    Id,
    UserId,
    Oid,
    GrantedBy,
    Actions,
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
    DatabaseId,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    TableId,
    FragmentTypeMask,
    DistributionType,
    StreamNode,
    VnodeMapping,
    StateTableIds,
    UpstreamFragmentId,
    DispatcherType,
    DistKeyIndices,
    OutputIndices,
}

#[derive(DeriveIden)]
enum Actor {
    Table,
    ActorId,
    FragmentId,
    Status,
    Splits,
    ParallelUnitId,
    UpstreamActorIds,
    Dispatchers,
    VnodeBitmap,
}

#[derive(DeriveIden)]
#[allow(clippy::enum_variant_names)]
enum Table {
    Table,
    TableId,
    Name,
    SchemaId,
    DatabaseId,
    OptionalAssociatedSourceId,
    TableType,
    Columns,
    Pk,
    DistributionKey,
    AppendOnly,
    Properties,
    FragmentId,
    VnodeColIndex,
    ValueIndices,
    Definition,
    HandlePkConflictBehavior,
    ReadPrefixLenHint,
    WatermarkIndices,
    DistKeyInPk,
    DmlFragmentId,
    Cardinality,
    CleanedByWatermark,
    Version,
}

#[derive(DeriveIden)]
enum Source {
    Table,
    SourceId,
    Name,
    SchemaId,
    DatabaseId,
    RowIdIndex,
    Columns,
    PkColumnIds,
    Properties,
    Definition,
    SourceInfo,
    WatermarkDescs,
    OptionalAssociatedTableId,
    ConnectionId,
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    SinkId,
    Name,
    SchemaId,
    DatabaseId,
    Columns,
    PkColumnIds,
    DistributionKey,
    DownstreamPk,
    SinkType,
    Properties,
    Definition,
    ConnectionId,
    DbName,
    SinkFromName,
}

#[derive(DeriveIden)]
enum Connection {
    Table,
    ConnectionId,
    Name,
    SchemaId,
    DatabaseId,
    Info,
}

#[derive(DeriveIden)]
enum View {
    Table,
    ViewId,
    Name,
    SchemaId,
    DatabaseId,
    Properties,
    Sql,
    Columns,
}

#[derive(DeriveIden)]
enum Index {
    Table,
    IndexId,
    Name,
    SchemaId,
    DatabaseId,
    IndexTableId,
    PrimaryTableId,
    IndexItems,
    OriginalColumns,
}

#[derive(DeriveIden)]
enum Function {
    Table,
    FunctionId,
    Name,
    SchemaId,
    DatabaseId,
    ArgTypes,
    ReturnType,
    Language,
    Link,
    Identifier,
    Kind,
}

#[derive(DeriveIden)]
enum Object {
    Table,
    Oid,
    ObjType,
    OwnerId,
    InitializedAt,
    CreatedAt,
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
enum ElectionLeader {
    Table,
    Service,
    Id,
    LastHeartbeat,
}

#[derive(DeriveIden)]
enum ElectionMember {
    Table,
    Service,
    Id,
    LastHeartbeat,
}
