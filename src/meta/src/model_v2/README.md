# How to define changes between versions and generate migration and model files

- Generate a new migration file and apply it to the database, check [migration](./migration/README.md) for more details. Let's take a local PG database as an example(`postgres://postgres:@localhost:5432/postgres`):
    ```sh
    export DATABASE_URL=postgres://postgres:@localhost:5432/postgres;
    cargo run -- generate MIGRATION_NAME
    cargo run -- up
    ```
  - Define tables, indexes, foreign keys in the file. The new generated file will include a sample migration script,
  you can replace it with your own migration scripts, like defining or changing tables, indexes, foreign keys and other
  dml operation to do data correctness etc. Check [writing-migration](https://www.sea-ql.org/SeaORM/docs/migration/writing-migration/)
  for more details.
  ```rust
    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts
        todo!();
      }

      async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
          // Replace the sample below with your own migration scripts
          todo!();
      }
    }
    ```
- Apply migration, and generate model files for new tables and indexes from the database, so you don't need to write them manually,
    ```sh
    cargo run -- up
    sea-orm-cli generate entity -u postgres://postgres:@localhost:5432/postgres -s public -o {target_dir}
    cp {target_dir}/xxx.rs src/meta/src/model_v2/
    ```
- Defines enum and array types in the model files, since they're basically only supported in PG, and we need to
  define them in the model files manually. For example:
  ```rust
  // We define integer array typed fields as json and derive it using the follow one.
  #[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Eq, Serialize, Deserialize, Default)]
  pub struct I32Array(pub Vec<i32>);

  // We define enum typed fields as string and derive it using the follow one.
  #[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
  #[sea_orm(rs_type = "String", db_type = "String(None)")]
  pub enum WorkerStatus {
  #[sea_orm(string_value = "STARTING")]
  Starting,
  #[sea_orm(string_value = "RUNNING")]
  Running,
  }
  ```
- Define other helper functions in the model files if necessary.