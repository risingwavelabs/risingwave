# risingwave_state_cleaning_test

The `risingwave_state_cleaning_test` crate has been designed specifically to test whether RisingWave can effectively clean outdated state records prior to reaching the watermark on time. Its functionality is described using TOML files, which specify the tests that should be executed. By utilizing this crate, developers can ensure that RisingWave is capable of properly managing state records, thereby improving overall application performance and providing a more reliable end-user experience.

## TOML files

The TOML files describe the tests that should be run. Each test is represented as a table in the TOML file with the following format:

```toml
[[test]]
name = "test name" # A human-readable name for the test
init_sqls = [ "SQL statement 1", "SQL statement 2", ... ] # A list of SQL statements to prepare the test environment
bound_tables = [
    { pattern = "table name pattern", limit = number }, # A pattern to match table names and a limit on the number of rows for each table
    { pattern = "table name pattern", limit = number },
    ...
] # A list of tables that should be checked.
```
