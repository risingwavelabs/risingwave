## ADDED Requirements

### Requirement: CREATE FUNCTION accepts secret-backed UDF options
The system MUST allow `CREATE FUNCTION` statements to bind UDF option values to catalog secrets by using `WITH (...)` entries in `SECRET <name>` form.

#### Scenario: User creates a function with a secret-backed option
- **WHEN** a user runs `CREATE FUNCTION ... WITH (api_token = SECRET my_schema.my_secret)`
- **THEN** the statement MUST be accepted if the referenced secret exists and is accessible to the user
- **AND** the function catalog entry MUST store a structured secret reference for `api_token` instead of a plaintext value

### Requirement: Secret references are validated during function DDL
The system MUST resolve secret names and enforce secret access checks during function creation so invalid references fail before the function is committed.

#### Scenario: Function creation references a non-existent secret
- **WHEN** a user runs `CREATE FUNCTION` with `SECRET` reference to a secret name that does not exist in the target database
- **THEN** the statement MUST fail with a catalog-not-found style error
- **AND** no function metadata MUST be persisted

#### Scenario: Function creation references an unauthorized secret
- **WHEN** a user runs `CREATE FUNCTION` with `SECRET` reference to a secret they are not allowed to use
- **THEN** the statement MUST fail with a permission error
- **AND** no function metadata MUST be persisted

### Requirement: Secret values are resolved at execution time without catalog plaintext storage
The system MUST keep plaintext secret values out of durable function metadata and resolve secret-backed UDF options from the secret manager only when building or executing the UDF runtime call.

#### Scenario: Function is executed after successful secret binding
- **WHEN** a query invokes a function that has secret-backed options
- **THEN** the runtime MUST obtain the plaintext value from secret manager using the stored secret reference metadata
- **AND** persisted function metadata MUST continue to contain secret references rather than plaintext credentials
