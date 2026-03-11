use std::collections::HashMap;

use sea_orm::{ConnectionTrait, FromQueryResult, Statement};
use sea_orm_migration::prelude::*;

const STREAMING_PARALLELISM: &str = "streaming_parallelism";
const STREAMING_PARALLELISM_FOR_TABLE: &str = "streaming_parallelism_for_table";
const STREAMING_PARALLELISM_FOR_SOURCE: &str = "streaming_parallelism_for_source";
const STREAMING_PARALLELISM_FOR_SINK: &str = "streaming_parallelism_for_sink";
const STREAMING_PARALLELISM_FOR_INDEX: &str = "streaming_parallelism_for_index";
const STREAMING_PARALLELISM_FOR_MATERIALIZED_VIEW: &str =
    "streaming_parallelism_for_materialized_view";
const DEFAULT_TABLE_PARALLELISM_BOUND: u64 = 4;
const DEFAULT_SOURCE_PARALLELISM_BOUND: u64 = 4;

const LEGACY_ADAPTIVE_PARALLELISM_STRATEGY: &str = "adaptive_parallelism_strategy";
const LEGACY_STREAMING_PARALLELISM_STRATEGY: &str = "streaming_parallelism_strategy";
const LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_TABLE: &str =
    "streaming_parallelism_strategy_for_table";
const LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SOURCE: &str =
    "streaming_parallelism_strategy_for_source";
const LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK: &str =
    "streaming_parallelism_strategy_for_sink";
const LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_INDEX: &str =
    "streaming_parallelism_strategy_for_index";
const LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_MATERIALIZED_VIEW: &str =
    "streaming_parallelism_strategy_for_materialized_view";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let legacy_system_strategy = load_legacy_system_strategy(manager).await?;
        migrate_legacy_streaming_parallelism_session_params(manager, legacy_system_strategy)
            .await?;
        migrate_legacy_streaming_job_strategy(manager, legacy_system_strategy).await?;
        delete_legacy_system_strategy(manager).await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "cannot rollback legacy streaming parallelism session parameter migration".to_owned(),
        ))?
    }
}

async fn migrate_legacy_streaming_parallelism_session_params(
    manager: &SchemaManager<'_>,
    legacy_system_strategy: Option<AdaptiveParallelismStrategy>,
) -> Result<(), DbErr> {
    let conn = manager.get_connection();
    let database_backend = conn.get_database_backend();

    let (sql, values) = Query::select()
        .columns([SessionParameter::Name, SessionParameter::Value])
        .from(SessionParameter::Table)
        .to_owned()
        .build_any(&*database_backend.get_query_builder());
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            database_backend,
            sql,
            values,
        ))
        .await?;
    let params = rows
        .into_iter()
        .map(|row| SessionParameterRow::from_query_result(&row, ""))
        .collect::<Result<Vec<_>, _>>()?;

    if !params
        .iter()
        .any(|param| is_legacy_streaming_parallelism_strategy_param(&param.name))
    {
        return Ok(());
    }

    let derived = derive_legacy_streaming_parallelism_params(&params, legacy_system_strategy);

    if !derived.is_empty() {
        let mut insert = Query::insert();
        insert
            .into_table(SessionParameter::Table)
            .columns([SessionParameter::Name, SessionParameter::Value])
            .on_conflict(
                sea_query::OnConflict::column(SessionParameter::Name)
                    .update_column(SessionParameter::Value)
                    .to_owned(),
            );

        for (name, value) in derived {
            insert.values_panic([name.into(), value.into()]);
        }

        manager.exec_stmt(insert.to_owned()).await?;
    }

    manager
        .exec_stmt(
            Query::delete()
                .from_table(SessionParameter::Table)
                .and_where(Expr::col(SessionParameter::Name).is_in([
                    LEGACY_STREAMING_PARALLELISM_STRATEGY,
                    LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_TABLE,
                    LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SOURCE,
                    LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK,
                    LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_INDEX,
                    LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_MATERIALIZED_VIEW,
                ]))
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn migrate_legacy_streaming_job_strategy(
    manager: &SchemaManager<'_>,
    legacy_system_strategy: Option<AdaptiveParallelismStrategy>,
) -> Result<(), DbErr> {
    let Some(strategy) = legacy_system_strategy else {
        return Ok(());
    };

    manager
        .exec_stmt(
            Query::update()
                .table(StreamingJob::Table)
                .value(
                    StreamingJob::AdaptiveParallelismStrategy,
                    strategy.to_string(),
                )
                .and_where(Expr::col(StreamingJob::AdaptiveParallelismStrategy).is_null())
                .to_owned(),
        )
        .await?;

    Ok(())
}

async fn delete_legacy_system_strategy(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .exec_stmt(
            Query::delete()
                .from_table(SystemParameter::Table)
                .and_where(
                    Expr::col(SystemParameter::Name).eq(LEGACY_ADAPTIVE_PARALLELISM_STRATEGY),
                )
                .to_owned(),
        )
        .await?;
    Ok(())
}

async fn load_legacy_system_strategy(
    manager: &SchemaManager<'_>,
) -> Result<Option<AdaptiveParallelismStrategy>, DbErr> {
    let conn = manager.get_connection();
    let database_backend = conn.get_database_backend();
    let (sql, values) = Query::select()
        .column(SystemParameter::Value)
        .from(SystemParameter::Table)
        .and_where(Expr::col(SystemParameter::Name).eq(LEGACY_ADAPTIVE_PARALLELISM_STRATEGY))
        .to_owned()
        .build_any(&*database_backend.get_query_builder());
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            database_backend,
            sql,
            values,
        ))
        .await?;

    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let row = SystemParameterRow::from_query_result(&row, "")?;

    Ok(parse_adaptive_parallelism_strategy(&row.value))
}

fn is_legacy_streaming_parallelism_strategy_param(name: &str) -> bool {
    matches!(
        name,
        LEGACY_STREAMING_PARALLELISM_STRATEGY
            | LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_TABLE
            | LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SOURCE
            | LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK
            | LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_INDEX
            | LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_MATERIALIZED_VIEW
    )
}

fn derive_legacy_streaming_parallelism_params(
    params: &[SessionParameterRow],
    legacy_system_strategy: Option<AdaptiveParallelismStrategy>,
) -> HashMap<String, String> {
    let Some(system_strategy) = legacy_system_strategy else {
        return HashMap::new();
    };

    let param_map = params
        .iter()
        .map(|param| (param.name.as_str(), param.value.as_str()))
        .collect::<HashMap<_, _>>();

    let global_parallelism = parse_parallelism(
        param_map.get(STREAMING_PARALLELISM).copied(),
        ConfigParallelism::Default,
    );
    let global_strategy = parse_legacy_strategy(
        param_map
            .get(LEGACY_STREAMING_PARALLELISM_STRATEGY)
            .copied(),
        ConfigAdaptiveParallelismStrategy::Default,
    );

    let mut derived = HashMap::from([(
        STREAMING_PARALLELISM.to_owned(),
        migrate_legacy_global_parallelism(global_parallelism, global_strategy, system_strategy)
            .to_string(),
    )]);

    for (parallelism_key, strategy_key) in [
        (
            STREAMING_PARALLELISM_FOR_TABLE,
            LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_TABLE,
        ),
        (
            STREAMING_PARALLELISM_FOR_SOURCE,
            LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SOURCE,
        ),
        (
            STREAMING_PARALLELISM_FOR_SINK,
            LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK,
        ),
        (
            STREAMING_PARALLELISM_FOR_INDEX,
            LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_INDEX,
        ),
        (
            STREAMING_PARALLELISM_FOR_MATERIALIZED_VIEW,
            LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_MATERIALIZED_VIEW,
        ),
    ] {
        let specific_parallelism = parse_parallelism(
            param_map.get(parallelism_key).copied(),
            ConfigParallelism::Default,
        );
        let specific_strategy = parse_legacy_strategy(
            param_map.get(strategy_key).copied(),
            default_legacy_strategy_for_type(parallelism_key),
        );
        derived.insert(
            parallelism_key.to_owned(),
            migrate_legacy_type_parallelism(
                specific_parallelism,
                specific_strategy,
                global_parallelism,
                global_strategy,
                system_strategy,
            )
            .to_string(),
        );
    }

    derived
}

fn default_legacy_strategy_for_type(parallelism_key: &str) -> ConfigAdaptiveParallelismStrategy {
    match parallelism_key {
        STREAMING_PARALLELISM_FOR_TABLE => {
            ConfigAdaptiveParallelismStrategy::Bounded(DEFAULT_TABLE_PARALLELISM_BOUND)
        }
        STREAMING_PARALLELISM_FOR_SOURCE => {
            ConfigAdaptiveParallelismStrategy::Bounded(DEFAULT_SOURCE_PARALLELISM_BOUND)
        }
        _ => ConfigAdaptiveParallelismStrategy::Default,
    }
}

fn parse_parallelism(value: Option<&str>, default: ConfigParallelism) -> ConfigParallelism {
    value.and_then(parse_config_parallelism).unwrap_or(default)
}

fn parse_legacy_strategy(
    value: Option<&str>,
    default: ConfigAdaptiveParallelismStrategy,
) -> ConfigAdaptiveParallelismStrategy {
    value
        .and_then(parse_config_adaptive_parallelism_strategy)
        .unwrap_or(default)
}

fn parse_config_parallelism(value: &str) -> Option<ConfigParallelism> {
    if value.eq_ignore_ascii_case("default") {
        return Some(ConfigParallelism::Default);
    }
    if value.eq_ignore_ascii_case("adaptive") || value.eq_ignore_ascii_case("auto") {
        return Some(ConfigParallelism::Adaptive);
    }
    if let Some(strategy) = parse_adaptive_parallelism_strategy(value) {
        return Some(match strategy {
            AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full => {
                ConfigParallelism::Adaptive
            }
            AdaptiveParallelismStrategy::Bounded(n) => ConfigParallelism::Bounded(n as u64),
            AdaptiveParallelismStrategy::Ratio(r) => ConfigParallelism::Ratio(r),
        });
    }

    let parsed = value.parse::<u64>().ok()?;
    Some(if parsed == 0 {
        ConfigParallelism::Adaptive
    } else {
        ConfigParallelism::Fixed(parsed)
    })
}

fn parse_config_adaptive_parallelism_strategy(
    value: &str,
) -> Option<ConfigAdaptiveParallelismStrategy> {
    if value.eq_ignore_ascii_case("default") {
        return Some(ConfigAdaptiveParallelismStrategy::Default);
    }
    Some(match parse_adaptive_parallelism_strategy(value)? {
        AdaptiveParallelismStrategy::Auto => ConfigAdaptiveParallelismStrategy::Auto,
        AdaptiveParallelismStrategy::Full => ConfigAdaptiveParallelismStrategy::Full,
        AdaptiveParallelismStrategy::Bounded(n) => {
            ConfigAdaptiveParallelismStrategy::Bounded(n as u64)
        }
        AdaptiveParallelismStrategy::Ratio(r) => ConfigAdaptiveParallelismStrategy::Ratio(r),
    })
}

fn parse_adaptive_parallelism_strategy(value: &str) -> Option<AdaptiveParallelismStrategy> {
    if value.eq_ignore_ascii_case("auto") {
        return Some(AdaptiveParallelismStrategy::Auto);
    }
    if value.eq_ignore_ascii_case("full") {
        return Some(AdaptiveParallelismStrategy::Full);
    }

    let lower = value.to_ascii_lowercase();
    if let Some(inner) = lower
        .strip_prefix("bounded(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let n = inner.parse::<usize>().ok()?;
        return (n > 0).then_some(AdaptiveParallelismStrategy::Bounded(n));
    }
    if let Some(inner) = lower
        .strip_prefix("ratio(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let r = inner.parse::<f32>().ok()?;
        return ((0.0..=1.0).contains(&r)).then_some(AdaptiveParallelismStrategy::Ratio(r));
    }

    None
}

fn migrate_legacy_global_parallelism(
    parallelism: ConfigParallelism,
    strategy: ConfigAdaptiveParallelismStrategy,
    system_strategy: AdaptiveParallelismStrategy,
) -> ConfigParallelism {
    match parallelism {
        ConfigParallelism::Fixed(_)
        | ConfigParallelism::Bounded(_)
        | ConfigParallelism::Ratio(_) => parallelism,
        ConfigParallelism::Default | ConfigParallelism::Adaptive => {
            legacy_strategy_to_parallelism(resolve_legacy_strategy(strategy, system_strategy))
        }
    }
}

fn migrate_legacy_type_parallelism(
    specific_parallelism: ConfigParallelism,
    specific_strategy: ConfigAdaptiveParallelismStrategy,
    global_parallelism: ConfigParallelism,
    global_strategy: ConfigAdaptiveParallelismStrategy,
    system_strategy: AdaptiveParallelismStrategy,
) -> ConfigParallelism {
    match specific_parallelism {
        ConfigParallelism::Fixed(_)
        | ConfigParallelism::Bounded(_)
        | ConfigParallelism::Ratio(_) => specific_parallelism,
        ConfigParallelism::Adaptive => legacy_strategy_to_parallelism(resolve_legacy_strategy(
            specific_strategy,
            resolve_legacy_strategy(global_strategy, system_strategy),
        )),
        ConfigParallelism::Default => {
            if matches!(
                specific_strategy,
                ConfigAdaptiveParallelismStrategy::Default
            ) || matches!(global_parallelism, ConfigParallelism::Fixed(_))
            {
                ConfigParallelism::Default
            } else {
                legacy_strategy_to_parallelism(resolve_legacy_strategy(
                    specific_strategy,
                    resolve_legacy_strategy(global_strategy, system_strategy),
                ))
            }
        }
    }
}

fn resolve_legacy_strategy(
    strategy: ConfigAdaptiveParallelismStrategy,
    fallback: AdaptiveParallelismStrategy,
) -> AdaptiveParallelismStrategy {
    match strategy {
        ConfigAdaptiveParallelismStrategy::Default => fallback,
        ConfigAdaptiveParallelismStrategy::Auto => AdaptiveParallelismStrategy::Auto,
        ConfigAdaptiveParallelismStrategy::Full => AdaptiveParallelismStrategy::Full,
        ConfigAdaptiveParallelismStrategy::Bounded(n) => {
            AdaptiveParallelismStrategy::Bounded(n as usize)
        }
        ConfigAdaptiveParallelismStrategy::Ratio(r) => AdaptiveParallelismStrategy::Ratio(r),
    }
}

fn legacy_strategy_to_parallelism(strategy: AdaptiveParallelismStrategy) -> ConfigParallelism {
    match strategy {
        AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full => {
            ConfigParallelism::Adaptive
        }
        AdaptiveParallelismStrategy::Bounded(n) => ConfigParallelism::Bounded(n as u64),
        AdaptiveParallelismStrategy::Ratio(r) => ConfigParallelism::Ratio(r),
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum ConfigParallelism {
    Default,
    Fixed(u64),
    Adaptive,
    Bounded(u64),
    Ratio(f32),
}

impl std::fmt::Display for ConfigParallelism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigParallelism::Default => write!(f, "default"),
            ConfigParallelism::Fixed(n) => write!(f, "{n}"),
            ConfigParallelism::Adaptive => write!(f, "adaptive"),
            ConfigParallelism::Bounded(n) => write!(f, "bounded({n})"),
            ConfigParallelism::Ratio(r) => write!(f, "ratio({r})"),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum ConfigAdaptiveParallelismStrategy {
    Default,
    Auto,
    Full,
    Bounded(u64),
    Ratio(f32),
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum AdaptiveParallelismStrategy {
    Auto,
    Full,
    Bounded(usize),
    Ratio(f32),
}

impl std::fmt::Display for AdaptiveParallelismStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdaptiveParallelismStrategy::Auto => write!(f, "AUTO"),
            AdaptiveParallelismStrategy::Full => write!(f, "FULL"),
            AdaptiveParallelismStrategy::Bounded(n) => write!(f, "BOUNDED({n})"),
            AdaptiveParallelismStrategy::Ratio(r) => write!(f, "RATIO({r})"),
        }
    }
}

#[derive(Debug, FromQueryResult)]
struct SessionParameterRow {
    name: String,
    value: String,
}

#[derive(Debug, FromQueryResult)]
struct SystemParameterRow {
    value: String,
}

#[derive(DeriveIden)]
enum SessionParameter {
    Table,
    Name,
    Value,
}

#[derive(DeriveIden)]
enum SystemParameter {
    Table,
    Name,
    Value,
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    AdaptiveParallelismStrategy,
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement};

    use super::*;
    use crate::{Migrator, MigratorTrait};

    async fn setup_db_before_target_migration() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let target_name = Migration.name();
        let target_index = Migrator::migrations()
            .iter()
            .position(|migration| migration.name() == target_name)
            .unwrap();
        Migrator::up(&db, Some(target_index as u32)).await.unwrap();
        db
    }

    async fn query_session_param(
        db: &DatabaseConnection,
        name: &str,
    ) -> Option<SessionParameterRow> {
        let backend = db.get_database_backend();
        let (sql, values) = Query::select()
            .columns([SessionParameter::Name, SessionParameter::Value])
            .from(SessionParameter::Table)
            .and_where(Expr::col(SessionParameter::Name).eq(name))
            .to_owned()
            .build_any(&*backend.get_query_builder());
        let rows = db
            .query_all(Statement::from_sql_and_values(backend, sql, values))
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|row| SessionParameterRow::from_query_result(&row, "").unwrap())
    }

    async fn query_system_param(db: &DatabaseConnection, name: &str) -> Option<SystemParameterRow> {
        let backend = db.get_database_backend();
        let (sql, values) = Query::select()
            .column(SystemParameter::Value)
            .from(SystemParameter::Table)
            .and_where(Expr::col(SystemParameter::Name).eq(name))
            .to_owned()
            .build_any(&*backend.get_query_builder());
        let rows = db
            .query_all(Statement::from_sql_and_values(backend, sql, values))
            .await
            .unwrap();
        rows.into_iter()
            .next()
            .map(|row| SystemParameterRow::from_query_result(&row, "").unwrap())
    }

    async fn query_streaming_job_strategy(db: &DatabaseConnection, job_id: i32) -> Option<String> {
        let backend = db.get_database_backend();
        let rows = db
            .query_all(Statement::from_string(
                backend,
                format!(
                    "SELECT adaptive_parallelism_strategy FROM streaming_job WHERE job_id = {job_id}"
                ),
            ))
            .await
            .unwrap();
        rows.into_iter().next().and_then(|row| {
            row.try_get::<Option<String>>("", "adaptive_parallelism_strategy")
                .unwrap()
        })
    }

    #[tokio::test]
    async fn test_migrate_legacy_streaming_parallelism_session_params() {
        let db = setup_db_before_target_migration().await;

        db.execute(Statement::from_string(
            db.get_database_backend(),
            format!(
                "INSERT INTO session_parameter (name, value) VALUES \
                 ('{STREAMING_PARALLELISM}', 'default'), \
                 ('{LEGACY_STREAMING_PARALLELISM_STRATEGY}', 'RATIO(0.5)'), \
                 ('{STREAMING_PARALLELISM_FOR_SINK}', 'default'), \
                 ('{LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK}', 'BOUNDED(4)')"
            ),
        ))
        .await
        .unwrap();
        db.execute(Statement::from_string(
            db.get_database_backend(),
            format!(
                "INSERT INTO system_parameter (name, value, is_mutable) VALUES \
                 ('{LEGACY_ADAPTIVE_PARALLELISM_STRATEGY}', 'AUTO', TRUE)"
            ),
        ))
        .await
        .unwrap();
        db.execute(Statement::from_string(
            db.get_database_backend(),
            "INSERT INTO user (user_id, name, is_super, can_create_db, can_create_user, can_login) VALUES \
             (999001, 'migration_test_user', TRUE, TRUE, TRUE, TRUE)"
                .to_owned(),
        ))
        .await
        .unwrap();
        db.execute(Statement::from_string(
            db.get_database_backend(),
            "INSERT INTO object (oid, obj_type, owner_id) VALUES \
             (999001, 'TABLE', 999001)"
                .to_owned(),
        ))
        .await
        .unwrap();
        db.execute(Statement::from_string(
            db.get_database_backend(),
            "INSERT INTO streaming_job (job_id, job_status, create_type, parallelism, max_parallelism, adaptive_parallelism_strategy, is_serverless_backfill) VALUES \
             (999001, 'CREATED', 'FOREGROUND', '\"Adaptive\"', 256, NULL, FALSE)"
                .to_owned(),
        ))
        .await
        .unwrap();

        Migrator::up(&db, Some(1)).await.unwrap();

        assert_eq!(
            query_session_param(&db, STREAMING_PARALLELISM)
                .await
                .unwrap()
                .value,
            "ratio(0.5)"
        );
        assert_eq!(
            query_session_param(&db, STREAMING_PARALLELISM_FOR_SINK)
                .await
                .unwrap()
                .value,
            "bounded(4)"
        );
        assert_eq!(
            query_session_param(&db, STREAMING_PARALLELISM_FOR_TABLE)
                .await
                .unwrap()
                .value,
            "bounded(4)"
        );
        assert_eq!(
            query_session_param(&db, STREAMING_PARALLELISM_FOR_SOURCE)
                .await
                .unwrap()
                .value,
            "bounded(4)"
        );
        assert!(
            query_session_param(&db, LEGACY_STREAMING_PARALLELISM_STRATEGY)
                .await
                .is_none()
        );
        assert!(
            query_session_param(&db, LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK)
                .await
                .is_none()
        );
        assert!(
            query_system_param(&db, LEGACY_ADAPTIVE_PARALLELISM_STRATEGY)
                .await
                .is_none()
        );
        assert_eq!(
            query_streaming_job_strategy(&db, 999001).await,
            Some("AUTO".to_owned())
        );
    }

    #[test]
    fn test_derive_legacy_streaming_parallelism_params_keeps_table_default_when_global_fixed() {
        let params = vec![SessionParameterRow {
            name: STREAMING_PARALLELISM.to_owned(),
            value: "8".to_owned(),
        }];

        let derived = derive_legacy_streaming_parallelism_params(
            &params,
            Some(AdaptiveParallelismStrategy::Auto),
        );

        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_TABLE)
                .map(String::as_str),
            Some("default")
        );
        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_SOURCE)
                .map(String::as_str),
            Some("default")
        );
    }

    #[test]
    fn test_derive_legacy_streaming_parallelism_params_uses_legacy_global_default_bound() {
        let params = vec![SessionParameterRow {
            name: LEGACY_STREAMING_PARALLELISM_STRATEGY.to_owned(),
            value: "default".to_owned(),
        }];

        let derived = derive_legacy_streaming_parallelism_params(
            &params,
            Some(AdaptiveParallelismStrategy::Bounded(64)),
        );

        assert_eq!(
            derived.get(STREAMING_PARALLELISM).map(String::as_str),
            Some("bounded(64)")
        );
        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_MATERIALIZED_VIEW)
                .map(String::as_str),
            Some("default")
        );
        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_SINK)
                .map(String::as_str),
            Some("default")
        );
        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_INDEX)
                .map(String::as_str),
            Some("default")
        );
        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_TABLE)
                .map(String::as_str),
            Some("bounded(4)")
        );
        assert_eq!(
            derived
                .get(STREAMING_PARALLELISM_FOR_SOURCE)
                .map(String::as_str),
            Some("bounded(4)")
        );
    }
}
