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
        let legacy_system_strategy = load_legacy_system_strategy(manager)
            .await?
            .unwrap_or_else(default_legacy_system_strategy);
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
    legacy_system_strategy: AdaptiveParallelismStrategy,
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
        .any(|param| is_migratable_streaming_parallelism_session_param(&param.name))
    {
        if legacy_system_strategy == default_legacy_system_strategy() {
            return Ok(());
        }

        manager
            .exec_stmt(
                Query::insert()
                    .into_table(SessionParameter::Table)
                    .columns([SessionParameter::Name, SessionParameter::Value])
                    .values_panic([
                        STREAMING_PARALLELISM.into(),
                        migrate_legacy_global_parallelism(
                            ConfigParallelism::Default,
                            ConfigAdaptiveParallelismStrategy::Default,
                            legacy_system_strategy,
                        )
                        .to_string()
                        .into(),
                    ])
                    .on_conflict(
                        sea_query::OnConflict::column(SessionParameter::Name)
                            .update_column(SessionParameter::Value)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
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
    legacy_system_strategy: AdaptiveParallelismStrategy,
) -> Result<(), DbErr> {
    // Existing jobs without a job-level strategy used to fall back to the legacy system
    // parameter. Materialize that fallback before dropping the system parameter so their
    // effective adaptive policy does not change after the migration.
    manager
        .exec_stmt(
            Query::update()
                .table(StreamingJob::Table)
                .value(
                    StreamingJob::AdaptiveParallelismStrategy,
                    legacy_system_strategy.to_string(),
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

fn default_legacy_system_strategy() -> AdaptiveParallelismStrategy {
    AdaptiveParallelismStrategy::Auto
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

fn is_streaming_parallelism_param(name: &str) -> bool {
    matches!(
        name,
        STREAMING_PARALLELISM
            | STREAMING_PARALLELISM_FOR_TABLE
            | STREAMING_PARALLELISM_FOR_SOURCE
            | STREAMING_PARALLELISM_FOR_SINK
            | STREAMING_PARALLELISM_FOR_INDEX
            | STREAMING_PARALLELISM_FOR_MATERIALIZED_VIEW
    )
}

fn is_migratable_streaming_parallelism_session_param(name: &str) -> bool {
    is_streaming_parallelism_param(name) || is_legacy_streaming_parallelism_strategy_param(name)
}

fn derive_legacy_streaming_parallelism_params(
    params: &[SessionParameterRow],
    legacy_system_strategy: AdaptiveParallelismStrategy,
) -> HashMap<String, String> {
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

    let mut derived = HashMap::new();
    if should_materialize_global_parallelism(
        global_parallelism,
        global_strategy,
        legacy_system_strategy,
    ) {
        derived.insert(
            STREAMING_PARALLELISM.to_owned(),
            migrate_legacy_global_parallelism(
                global_parallelism,
                global_strategy,
                legacy_system_strategy,
            )
            .to_string(),
        );
    }

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
        let implicit_legacy_strategy = default_legacy_strategy_for_type(parallelism_key);
        let specific_strategy_value = param_map.get(strategy_key).copied();
        let specific_parallelism = parse_parallelism(
            param_map.get(parallelism_key).copied(),
            ConfigParallelism::Default,
        );
        let specific_strategy =
            parse_legacy_strategy(specific_strategy_value, implicit_legacy_strategy);
        let explicit_default_inherits_global = matches!(
            specific_strategy_value,
            Some(value) if value.eq_ignore_ascii_case("default")
        ) && !matches!(
            implicit_legacy_strategy,
            ConfigAdaptiveParallelismStrategy::Default
        );
        derived.insert(
            parallelism_key.to_owned(),
            migrate_legacy_type_parallelism(
                specific_parallelism,
                specific_strategy,
                explicit_default_inherits_global,
                global_parallelism,
                global_strategy,
                legacy_system_strategy,
            )
            .to_string(),
        );
    }

    derived
}

fn should_materialize_global_parallelism(
    global_parallelism: ConfigParallelism,
    global_strategy: ConfigAdaptiveParallelismStrategy,
    legacy_system_strategy: AdaptiveParallelismStrategy,
) -> bool {
    !matches!(global_parallelism, ConfigParallelism::Default)
        || !matches!(global_strategy, ConfigAdaptiveParallelismStrategy::Default)
        || legacy_system_strategy != default_legacy_system_strategy()
}

fn default_legacy_strategy_for_type(_parallelism_key: &str) -> ConfigAdaptiveParallelismStrategy {
    ConfigAdaptiveParallelismStrategy::Default
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
    explicit_default_inherits_global: bool,
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
            if matches!(global_parallelism, ConfigParallelism::Fixed(_)) {
                ConfigParallelism::Default
            } else if explicit_default_inherits_global {
                legacy_strategy_to_parallelism(resolve_legacy_strategy(
                    global_strategy,
                    system_strategy,
                ))
            } else if matches!(
                specific_strategy,
                ConfigAdaptiveParallelismStrategy::Default
            ) {
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
    use super::*;

    fn session_param(name: &str, value: &str) -> SessionParameterRow {
        SessionParameterRow {
            name: name.to_owned(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn test_derive_legacy_streaming_parallelism_params_type_only_keeps_global_untouched() {
        let derived = derive_legacy_streaming_parallelism_params(
            &[session_param(
                LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK,
                "bounded(8)",
            )],
            AdaptiveParallelismStrategy::Auto,
        );

        assert_eq!(derived.get(STREAMING_PARALLELISM), None);
        assert_eq!(
            derived.get(STREAMING_PARALLELISM_FOR_SINK),
            Some(&"bounded(8)".to_owned())
        );
        assert_eq!(
            derived.get(STREAMING_PARALLELISM_FOR_TABLE),
            Some(&"default".to_owned())
        );
    }

    #[test]
    fn test_derive_legacy_streaming_parallelism_params_materializes_custom_system_strategy() {
        let derived = derive_legacy_streaming_parallelism_params(
            &[session_param(
                LEGACY_STREAMING_PARALLELISM_STRATEGY_FOR_SINK,
                "bounded(8)",
            )],
            AdaptiveParallelismStrategy::Bounded(16),
        );

        assert_eq!(
            derived.get(STREAMING_PARALLELISM),
            Some(&"bounded(16)".to_owned())
        );
        assert_eq!(
            derived.get(STREAMING_PARALLELISM_FOR_SINK),
            Some(&"bounded(8)".to_owned())
        );
    }
}
