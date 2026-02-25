## ADDED Requirements

### Requirement: Embedded UDF runtimes MUST expose resolved secret-backed options to function code
The system MUST make `CREATE FUNCTION ... WITH (...)` secret-backed option values available to embedded runtime function code at execution time through a stable read-only runtime variable.

#### Scenario: Python UDF reads a secret-backed option
- **WHEN** a Python UDF is created with `WITH (api_token = SECRET my_schema.my_secret)`
- **THEN** the Python runtime MUST expose `api_token` with the resolved plaintext value through the documented runtime options variable during function execution
- **AND** the function call MUST succeed when it uses that value in user code

#### Scenario: JavaScript UDF reads a secret-backed option
- **WHEN** a JavaScript UDF is created with `WITH (api_token = SECRET my_schema.my_secret)`
- **THEN** the JavaScript runtime MUST expose `api_token` with the resolved plaintext value through the documented runtime options variable during function execution
- **AND** the function call MUST succeed when it uses that value in user code

### Requirement: Functions without secret-backed options MUST preserve existing behavior
The system MUST keep runtime behavior unchanged for functions that do not define secret-backed custom options.

#### Scenario: Existing UDF without custom secret options
- **WHEN** a function is created without secret-backed custom options
- **THEN** runtime initialization and function execution MUST behave the same as before this change
- **AND** no new user-visible options variable content is required for that function

### Requirement: Non-secret custom option values MUST remain rejected
The system MUST continue rejecting non-secret custom `WITH (...)` option values for functions.

#### Scenario: Plaintext custom option in WITH clause
- **WHEN** a user creates a function with `WITH (api_token = literal)`
- **THEN** statement validation MUST fail before function creation
- **AND** no function metadata MUST be persisted
