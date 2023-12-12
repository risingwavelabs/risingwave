pub use sea_orm_migration::prelude::*;

mod m20230908_072257_init;
mod m20231008_020431_hummock;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230908_072257_init::Migration),
            Box::new(m20231008_020431_hummock::Migration),
        ]
    }
}
