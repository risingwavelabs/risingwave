use sea_orm_migration::prelude::*;

#[tokio::main]
async fn main() {
    cli::run_cli(risingwave_meta_model_migration::Migrator).await;
}
