# Running Migrator CLI

> **WARNING:** Migration files are used to define schema changes for the database. Each migration file contains an up and down function,
> which are used to define upgrade and downgrade operations for the schema.
>
> When you need to make schema changes to the system catalog, you need to generate a new migration file and then apply it to the database.
> Note that each migration file can only be applied once and will be recorded in a system table, so for new schema changes, you need to
> generate a new migration file. Unless you are sure the modification of the migration file has not been included in any released version yet,
> **DO NOT** modify already published migration files.

## How to run the migrator CLI

- Generate a new migration file, a database endpoint is required but not used.
  Run this command in this directory, not project root.
    ```sh
    DATABASE_URL=sqlite::memory: cargo run -- generate MIGRATION_NAME
    ```
- Apply all pending migrations for test purposes, change `DATABASE_URL` to the actual database endpoint.
    ```sh
    cargo run
    ```
    ```sh
    cargo run -- up
    ```
- Apply first 10 pending migrations
    ```sh
    cargo run -- up -n 10
    ```
- Rollback last applied migrations
    ```sh
    cargo run -- down
    ```
- Rollback last 10 applied migrations
    ```sh
    cargo run -- down -n 10
    ```
- Drop all tables from the database, then reapply all migrations
    ```sh
    cargo run -- fresh
    ```
- Rollback all applied migrations, then reapply all migrations
    ```sh
    cargo run -- refresh
    ```
- Rollback all applied migrations
    ```sh
    cargo run -- reset
    ```
- Check the status of all migrations
    ```sh
    cargo run -- status
    ```

## Adding a migration

- Add a new column to some catalogs. You can checkout the migration [m20240617_070131_index_column_properties.rs](src/m20240617_070131_index_column_properties.rs) as a reference.
