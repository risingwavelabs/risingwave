## ADDED Requirements

### Requirement: Function-secret dependencies are recorded on function creation
The system MUST record dependency edges from each referenced secret to the created function whenever a function definition includes secret-backed options.

#### Scenario: Function creation includes multiple secret references
- **WHEN** a user creates a function that references two or more secrets in `WITH (...)`
- **THEN** the catalog MUST create one dependency edge per referenced secret to the function object
- **AND** dependency metadata MUST be committed atomically with function creation

### Requirement: Secret drop is blocked by dependent functions in restrict mode
The system MUST reject secret drop operations that would break live function dependencies unless dependent functions are removed first.

#### Scenario: User drops a secret referenced by an existing function
- **WHEN** `DROP SECRET <name>` is executed for a secret currently referenced by at least one function
- **THEN** the statement MUST fail in restrict mode
- **AND** the error MUST identify that dependent function objects exist and require explicit cleanup

### Requirement: Dependency cleanup follows function lifecycle
The system MUST remove function-secret dependency edges when the corresponding function object is dropped.

#### Scenario: Function drop unblocks subsequent secret drop
- **WHEN** a user drops a function that is the only dependent object of a secret
- **THEN** the dependency edge between that function and secret MUST be removed as part of the drop lifecycle
- **AND** a subsequent `DROP SECRET <name>` in restrict mode MUST succeed
