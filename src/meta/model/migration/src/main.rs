use sea_orm_migration::prelude::*;

fn main() {
    smol::block_on(async {
        cli::run_cli(risingwave_meta_model_migration::Migrator).await;
    });
}
