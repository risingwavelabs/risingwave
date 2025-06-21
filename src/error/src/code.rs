// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Represents all possible PostgreSQL error codes as defined in Table A.1.
///
/// Each variant corresponds to a specific `Condition Name` from the PostgreSQL documentation.
/// This enum provides a type-safe way to handle and match on specific SQLSTATE error codes.
///
/// See: <https://www.postgresql.org/docs/13/errcodes-appendix.html>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum PostgresErrorCode {
    // Class 00 — Successful Completion
    /// **Code `00000`**: `successful_completion`
    SuccessfulCompletion,

    // Class 01 — Warning
    /// **Code `01000`**: `warning`
    Warning,
    /// **Code `0100C`**: `dynamic_result_sets_returned`
    DynamicResultSetsReturned,
    /// **Code `01008`**: `implicit_zero_bit_padding`
    ImplicitZeroBitPadding,
    /// **Code `01003`**: `null_value_eliminated_in_set_function`
    NullValueEliminatedInSetFunction,
    /// **Code `01007`**: `privilege_not_granted`
    PrivilegeNotGranted,
    /// **Code `01006`**: `privilege_not_revoked`
    PrivilegeNotRevoked,
    /// **Code `01004`**: `string_data_right_truncation`
    StringDataRightTruncation01004,
    /// **Code `01P01`**: `deprecated_feature`
    DeprecatedFeature,

    // Class 02 — No Data
    /// **Code `02000`**: `no_data`
    NoData,
    /// **Code `02001`**: `no_additional_dynamic_result_sets_returned`
    NoAdditionalDynamicResultSetsReturned,

    // Class 03 — SQL Statement Not Yet Complete
    /// **Code `03000`**: `sql_statement_not_yet_complete`
    SqlStatementNotYetComplete,

    // Class 08 — Connection Exception
    /// **Code `08000`**: `connection_exception`
    ConnectionException,
    /// **Code `08003`**: `connection_does_not_exist`
    ConnectionDoesNotExist,
    /// **Code `08006`**: `connection_failure`
    ConnectionFailure,
    /// **Code `08001`**: `sqlclient_unable_to_establish_sqlconnection`
    SqlclientUnableToEstablishSqlconnection,
    /// **Code `08004`**: `sqlserver_rejected_establishment_of_sqlconnection`
    SqlserverRejectedEstablishmentOfSqlconnection,
    /// **Code `08007`**: `transaction_resolution_unknown`
    TransactionResolutionUnknown,
    /// **Code `08P01`**: `protocol_violation`
    ProtocolViolation,

    // Class 09 — Triggered Action Exception
    /// **Code `09000`**: `triggered_action_exception`
    TriggeredActionException,

    // Class 0A — Feature Not Supported
    /// **Code `0A000`**: `feature_not_supported`
    FeatureNotSupported,

    // Class 0B — Invalid Transaction Initiation
    /// **Code `0B000`**: `invalid_transaction_initiation`
    InvalidTransactionInitiation,

    // Class 0F — Locator Exception
    /// **Code `0F000`**: `locator_exception`
    LocatorException,
    /// **Code `0F001`**: `invalid_locator_specification`
    InvalidLocatorSpecification,

    // Class 0L — Invalid Grantor
    /// **Code `0L000`**: `invalid_grantor`
    InvalidGrantor,
    /// **Code `0LP01`**: `invalid_grant_operation`
    InvalidGrantOperation,

    // Class 0P — Invalid Role Specification
    /// **Code `0P000`**: `invalid_role_specification`
    InvalidRoleSpecification,

    // Class 0Z — Diagnostics Exception
    /// **Code `0Z000`**: `diagnostics_exception`
    DiagnosticsException,
    /// **Code `0Z002`**: `stacked_diagnostics_accessed_without_active_handler`
    StackedDiagnosticsAccessedWithoutActiveHandler,

    // Class 20 — Case Not Found
    /// **Code `20000`**: `case_not_found`
    CaseNotFound,

    // Class 21 — Cardinality Violation
    /// **Code `21000`**: `cardinality_violation`
    CardinalityViolation,

    // Class 22 — Data Exception
    /// **Code `22000`**: `data_exception`
    DataException,
    /// **Code `2202E`**: `array_subscript_error`
    ArraySubscriptError,
    /// **Code `22021`**: `character_not_in_repertoire`
    CharacterNotInRepertoire,
    /// **Code `22008`**: `datetime_field_overflow`
    DatetimeFieldOverflow,
    /// **Code `22012`**: `division_by_zero`
    DivisionByZero,
    /// **Code `22005`**: `error_in_assignment`
    ErrorInAssignment,
    /// **Code `2200B`**: `escape_character_conflict`
    EscapeCharacterConflict,
    /// **Code `22022`**: `indicator_overflow`
    IndicatorOverflow,
    /// **Code `22015`**: `interval_field_overflow`
    IntervalFieldOverflow,
    /// **Code `2201E`**: `invalid_argument_for_logarithm`
    InvalidArgumentForLogarithm,
    /// **Code `22014`**: `invalid_argument_for_ntile_function`
    InvalidArgumentForNtileFunction,
    /// **Code `22016`**: `invalid_argument_for_nth_value_function`
    InvalidArgumentForNthValueFunction,
    /// **Code `2201F`**: `invalid_argument_for_power_function`
    InvalidArgumentForPowerFunction,
    /// **Code `2201G`**: `invalid_argument_for_width_bucket_function`
    InvalidArgumentForWidthBucketFunction,
    /// **Code `22018`**: `invalid_character_value_for_cast`
    InvalidCharacterValueForCast,
    /// **Code `22007`**: `invalid_datetime_format`
    InvalidDatetimeFormat,
    /// **Code `22019`**: `invalid_escape_character`
    InvalidEscapeCharacter,
    /// **Code `2200D`**: `invalid_escape_octet`
    InvalidEscapeOctet,
    /// **Code `22025`**: `invalid_escape_sequence`
    InvalidEscapeSequence,
    /// **Code `22P06`**: `nonstandard_use_of_escape_character`
    NonstandardUseOfEscapeCharacter,
    /// **Code `22010`**: `invalid_indicator_parameter_value`
    InvalidIndicatorParameterValue,
    /// **Code `22023`**: `invalid_parameter_value`
    InvalidParameterValue,
    /// **Code `22013`**: `invalid_preceding_or_following_size`
    InvalidPrecedingOrFollowingSize,
    /// **Code `2201B`**: `invalid_regular_expression`
    InvalidRegularExpression,
    /// **Code `2201W`**: `invalid_row_count_in_limit_clause`
    InvalidRowCountInLimitClause,
    /// **Code `2201X`**: `invalid_row_count_in_result_offset_clause`
    InvalidRowCountInResultOffsetClause,
    /// **Code `2202H`**: `invalid_tablesample_argument`
    InvalidTablesampleArgument,
    /// **Code `2202G`**: `invalid_tablesample_repeat`
    InvalidTablesampleRepeat,
    /// **Code `22009`**: `invalid_time_zone_displacement_value`
    InvalidTimeZoneDisplacementValue,
    /// **Code `2200C`**: `invalid_use_of_escape_character`
    InvalidUseOfEscapeCharacter,
    /// **Code `2200G`**: `most_specific_type_mismatch`
    MostSpecificTypeMismatch,
    /// **Code `22004`**: `null_value_not_allowed`
    NullValueNotAllowed22004,
    /// **Code `22002`**: `null_value_no_indicator_parameter`
    NullValueNoIndicatorParameter,
    /// **Code `22003`**: `numeric_value_out_of_range`
    NumericValueOutOfRange,
    /// **Code `2200H`**: `sequence_generator_limit_exceeded`
    SequenceGeneratorLimitExceeded,
    /// **Code `22026`**: `string_data_length_mismatch`
    StringDataLengthMismatch,
    /// **Code `22001`**: `string_data_right_truncation`
    StringDataRightTruncation22001,
    /// **Code `22011`**: `substring_error`
    SubstringError,
    /// **Code `22027`**: `trim_error`
    TrimError,
    /// **Code `22024`**: `unterminated_c_string`
    UnterminatedCString,
    /// **Code `2200F`**: `zero_length_character_string`
    ZeroLengthCharacterString,
    /// **Code `22P01`**: `floating_point_exception`
    FloatingPointException,
    /// **Code `22P02`**: `invalid_text_representation`
    InvalidTextRepresentation,
    /// **Code `22P03`**: `invalid_binary_representation`
    InvalidBinaryRepresentation,
    /// **Code `22P04`**: `bad_copy_file_format`
    BadCopyFileFormat,
    /// **Code `22P05`**: `untranslatable_character`
    UntranslatableCharacter,
    /// **Code `2200L`**: `not_an_xml_document`
    NotAnXmlDocument,
    /// **Code `2200M`**: `invalid_xml_document`
    InvalidXmlDocument,
    /// **Code `2200N`**: `invalid_xml_content`
    InvalidXmlContent,
    /// **Code `2200S`**: `invalid_xml_comment`
    InvalidXmlComment,
    /// **Code `2200T`**: `invalid_xml_processing_instruction`
    InvalidXmlProcessingInstruction,
    /// **Code `22030`**: `duplicate_json_object_key_value`
    DuplicateJsonObjectKeyValue,
    /// **Code `22031`**: `invalid_argument_for_sql_json_datetime_function`
    InvalidArgumentForSqlJsonDatetimeFunction,
    /// **Code `22032`**: `invalid_json_text`
    InvalidJsonText,
    /// **Code `22033`**: `invalid_sql_json_subscript`
    InvalidSqlJsonSubscript,
    /// **Code `22034`**: `more_than_one_sql_json_item`
    MoreThanOneSqlJsonItem,
    /// **Code `22035`**: `no_sql_json_item`
    NoSqlJsonItem,
    /// **Code `22036`**: `non_numeric_sql_json_item`
    NonNumericSqlJsonItem,
    /// **Code `22037`**: `non_unique_keys_in_a_json_object`
    NonUniqueKeysInAJsonObject,
    /// **Code `22038`**: `singleton_sql_json_item_required`
    SingletonSqlJsonItemRequired,
    /// **Code `22039`**: `sql_json_array_not_found`
    SqlJsonArrayNotFound,
    /// **Code `2203A`**: `sql_json_member_not_found`
    SqlJsonMemberNotFound,
    /// **Code `2203B`**: `sql_json_number_not_found`
    SqlJsonNumberNotFound,
    /// **Code `2203C`**: `sql_json_object_not_found`
    SqlJsonObjectNotFound,
    /// **Code `2203D`**: `too_many_json_array_elements`
    TooManyJsonArrayElements,
    /// **Code `2203E`**: `too_many_json_object_members`
    TooManyJsonObjectMembers,
    /// **Code `2203F`**: `sql_json_scalar_required`
    SqlJsonScalarRequired,

    // Class 23 — Integrity Constraint Violation
    /// **Code `23000`**: `integrity_constraint_violation`
    IntegrityConstraintViolation,
    /// **Code `23001`**: `restrict_violation`
    RestrictViolation,
    /// **Code `23502`**: `not_null_violation`
    NotNullViolation,
    /// **Code `23503`**: `foreign_key_violation`
    ForeignKeyViolation,
    /// **Code `23505`**: `unique_violation`
    UniqueViolation,
    /// **Code `23514`**: `check_violation`
    CheckViolation,
    /// **Code `23P01`**: `exclusion_violation`
    ExclusionViolation,

    // Class 24 — Invalid Cursor State
    /// **Code `24000`**: `invalid_cursor_state`
    InvalidCursorState,

    // Class 25 — Invalid Transaction State
    /// **Code `25000`**: `invalid_transaction_state`
    InvalidTransactionState,
    /// **Code `25001`**: `active_sql_transaction`
    ActiveSqlTransaction,
    /// **Code `25002`**: `branch_transaction_already_active`
    BranchTransactionAlreadyActive,
    /// **Code `25008`**: `held_cursor_requires_same_isolation_level`
    HeldCursorRequiresSameIsolationLevel,
    /// **Code `25003`**: `inappropriate_access_mode_for_branch_transaction`
    InappropriateAccessModeForBranchTransaction,
    /// **Code `25004`**: `inappropriate_isolation_level_for_branch_transaction`
    InappropriateIsolationLevelForBranchTransaction,
    /// **Code `25005`**: `no_active_sql_transaction_for_branch_transaction`
    NoActiveSqlTransactionForBranchTransaction,
    /// **Code `25006`**: `read_only_sql_transaction`
    ReadOnlySqlTransaction,
    /// **Code `25007`**: `schema_and_data_statement_mixing_not_supported`
    SchemaAndDataStatementMixingNotSupported,
    /// **Code `25P01`**: `no_active_sql_transaction`
    NoActiveSqlTransaction,
    /// **Code `25P02`**: `in_failed_sql_transaction`
    InFailedSqlTransaction,
    /// **Code `25P03`**: `idle_in_transaction_session_timeout`
    IdleInTransactionSessionTimeout,

    // Class 26 — Invalid SQL Statement Name
    /// **Code `26000`**: `invalid_sql_statement_name`
    InvalidSqlStatementName,

    // Class 27 — Triggered Data Change Violation
    /// **Code `27000`**: `triggered_data_change_violation`
    TriggeredDataChangeViolation,

    // Class 28 — Invalid Authorization Specification
    /// **Code `28000`**: `invalid_authorization_specification`
    InvalidAuthorizationSpecification,
    /// **Code `28P01`**: `invalid_password`
    InvalidPassword,

    // Class 2B — Dependent Privilege Descriptors Still Exist
    /// **Code `2B000`**: `dependent_privilege_descriptors_still_exist`
    DependentPrivilegeDescriptorsStillExist,
    /// **Code `2BP01`**: `dependent_objects_still_exist`
    DependentObjectsStillExist,

    // Class 2D — Invalid Transaction Termination
    /// **Code `2D000`**: `invalid_transaction_termination`
    InvalidTransactionTermination,

    // Class 2F — SQL Routine Exception
    /// **Code `2F000`**: `sql_routine_exception`
    SqlRoutineException,
    /// **Code `2F005`**: `function_executed_no_return_statement`
    FunctionExecutedNoReturnStatement,
    /// **Code `2F002`**: `modifying_sql_data_not_permitted`
    ModifyingSqlDataNotPermitted2F002,
    /// **Code `2F003`**: `prohibited_sql_statement_attempted`
    ProhibitedSqlStatementAttempted2F003,
    /// **Code `2F004`**: `reading_sql_data_not_permitted`
    ReadingSqlDataNotPermitted2F004,

    // Class 34 — Invalid Cursor Name
    /// **Code `34000`**: `invalid_cursor_name`
    InvalidCursorName,

    // Class 38 — External Routine Exception
    /// **Code `38000`**: `external_routine_exception`
    ExternalRoutineException,
    /// **Code `38001`**: `containing_sql_not_permitted`
    ContainingSqlNotPermitted,
    /// **Code `38002`**: `modifying_sql_data_not_permitted`
    ModifyingSqlDataNotPermitted38002,
    /// **Code `38003`**: `prohibited_sql_statement_attempted`
    ProhibitedSqlStatementAttempted38003,
    /// **Code `38004`**: `reading_sql_data_not_permitted`
    ReadingSqlDataNotPermitted38004,

    // Class 39 — External Routine Invocation Exception
    /// **Code `39000`**: `external_routine_invocation_exception`
    ExternalRoutineInvocationException,
    /// **Code `39001`**: `invalid_sqlstate_returned`
    InvalidSqlstateReturned,
    /// **Code `39004`**: `null_value_not_allowed`
    NullValueNotAllowed39004,
    /// **Code `39P01`**: `trigger_protocol_violated`
    TriggerProtocolViolated,
    /// **Code `39P02`**: `srf_protocol_violated`
    SrfProtocolViolated,
    /// **Code `39P03`**: `event_trigger_protocol_violated`
    EventTriggerProtocolViolated,

    // Class 3B — Savepoint Exception
    /// **Code `3B000`**: `savepoint_exception`
    SavepointException,
    /// **Code `3B001`**: `invalid_savepoint_specification`
    InvalidSavepointSpecification,

    // Class 3D — Invalid Catalog Name
    /// **Code `3D000`**: `invalid_catalog_name`
    InvalidCatalogName,

    // Class 3F — Invalid Schema Name
    /// **Code `3F000`**: `invalid_schema_name`
    InvalidSchemaName,

    // Class 40 — Transaction Rollback
    /// **Code `40000`**: `transaction_rollback`
    TransactionRollback,
    /// **Code `40002`**: `transaction_integrity_constraint_violation`
    TransactionIntegrityConstraintViolation,
    /// **Code `40001`**: `serialization_failure`
    SerializationFailure,
    /// **Code `40003`**: `statement_completion_unknown`
    StatementCompletionUnknown,
    /// **Code `40P01`**: `deadlock_detected`
    DeadlockDetected,

    // Class 42 — Syntax Error or Access Rule Violation
    /// **Code `42000`**: `syntax_error_or_access_rule_violation`
    SyntaxErrorOrAccessRuleViolation,
    /// **Code `42601`**: `syntax_error`
    SyntaxError,
    /// **Code `42501`**: `insufficient_privilege`
    InsufficientPrivilege,
    /// **Code `42846`**: `cannot_coerce`
    CannotCoerce,
    /// **Code `42803`**: `grouping_error`
    GroupingError,
    /// **Code `42P20`**: `windowing_error`
    WindowingError,
    /// **Code `42P19`**: `invalid_recursion`
    InvalidRecursion,
    /// **Code `42830`**: `invalid_foreign_key`
    InvalidForeignKey,
    /// **Code `42602`**: `invalid_name`
    InvalidName,
    /// **Code `42622`**: `name_too_long`
    NameTooLong,
    /// **Code `42939`**: `reserved_name`
    ReservedName,
    /// **Code `42804`**: `datatype_mismatch`
    DatatypeMismatch,
    /// **Code `42P18`**: `indeterminate_datatype`
    IndeterminateDatatype,
    /// **Code `42P21`**: `collation_mismatch`
    CollationMismatch,
    /// **Code `42P22`**: `indeterminate_collation`
    IndeterminateCollation,
    /// **Code `42809`**: `wrong_object_type`
    WrongObjectType,
    /// **Code `428C9`**: `generated_always`
    GeneratedAlways,
    /// **Code `42703`**: `undefined_column`
    UndefinedColumn,
    /// **Code `42883`**: `undefined_function`
    UndefinedFunction,
    /// **Code `42P01`**: `undefined_table`
    UndefinedTable,
    /// **Code `42P02`**: `undefined_parameter`
    UndefinedParameter,
    /// **Code `42704`**: `undefined_object`
    UndefinedObject,
    /// **Code `42701`**: `duplicate_column`
    DuplicateColumn,
    /// **Code `42P03`**: `duplicate_cursor`
    DuplicateCursor,
    /// **Code `42P04`**: `duplicate_database`
    DuplicateDatabase,
    /// **Code `42723`**: `duplicate_function`
    DuplicateFunction,
    /// **Code `42P05`**: `duplicate_prepared_statement`
    DuplicatePreparedStatement,
    /// **Code `42P06`**: `duplicate_schema`
    DuplicateSchema,
    /// **Code `42P07`**: `duplicate_table`
    DuplicateTable,
    /// **Code `42712`**: `duplicate_alias`
    DuplicateAlias,
    /// **Code `42710`**: `duplicate_object`
    DuplicateObject,
    /// **Code `42702`**: `ambiguous_column`
    AmbiguousColumn,
    /// **Code `42725`**: `ambiguous_function`
    AmbiguousFunction,
    /// **Code `42P08`**: `ambiguous_parameter`
    AmbiguousParameter,
    /// **Code `42P09`**: `ambiguous_alias`
    AmbiguousAlias,
    /// **Code `42P10`**: `invalid_column_reference`
    InvalidColumnReference,
    /// **Code `42611`**: `invalid_column_definition`
    InvalidColumnDefinition,
    /// **Code `42P11`**: `invalid_cursor_definition`
    InvalidCursorDefinition,
    /// **Code `42P12`**: `invalid_database_definition`
    InvalidDatabaseDefinition,
    /// **Code `42P13`**: `invalid_function_definition`
    InvalidFunctionDefinition,
    /// **Code `42P14`**: `invalid_prepared_statement_definition`
    InvalidPreparedStatementDefinition,
    /// **Code `42P15`**: `invalid_schema_definition`
    InvalidSchemaDefinition,
    /// **Code `42P16`**: `invalid_table_definition`
    InvalidTableDefinition,
    /// **Code `42P17`**: `invalid_object_definition`
    InvalidObjectDefinition,

    // Class 44 — WITH CHECK OPTION Violation
    /// **Code `44000`**: `with_check_option_violation`
    WithCheckOptionViolation,

    // Class 53 — Insufficient Resources
    /// **Code `53000`**: `insufficient_resources`
    InsufficientResources,
    /// **Code `53100`**: `disk_full`
    DiskFull,
    /// **Code `53200`**: `out_of_memory`
    OutOfMemory53200,
    /// **Code `53300`**: `too_many_connections`
    TooManyConnections,
    /// **Code `53400`**: `configuration_limit_exceeded`
    ConfigurationLimitExceeded,

    // Class 54 — Program Limit Exceeded
    /// **Code `54000`**: `program_limit_exceeded`
    ProgramLimitExceeded,
    /// **Code `54001`**: `statement_too_complex`
    StatementTooComplex,
    /// **Code `54011`**: `too_many_columns`
    TooManyColumns,
    /// **Code `54023`**: `too_many_arguments`
    TooManyArguments,

    // Class 55 — Object Not In Prerequisite State
    /// **Code `55000`**: `object_not_in_prerequisite_state`
    ObjectNotInPrerequisiteState,
    /// **Code `55006`**: `object_in_use`
    ObjectInUse,
    /// **Code `55P02`**: `cant_change_runtime_param`
    CantChangeRuntimeParam,
    /// **Code `55P03`**: `lock_not_available`
    LockNotAvailable,
    /// **Code `55P04`**: `unsafe_new_enum_value_usage`
    UnsafeNewEnumValueUsage,

    // Class 57 — Operator Intervention
    /// **Code `57000`**: `operator_intervention`
    OperatorIntervention,
    /// **Code `57014`**: `query_canceled`
    QueryCanceled,
    /// **Code `57P01`**: `admin_shutdown`
    AdminShutdown,
    /// **Code `57P02`**: `crash_shutdown`
    CrashShutdown,
    /// **Code `57P03`**: `cannot_connect_now`
    CannotConnectNow,
    /// **Code `57P04`**: `database_dropped`
    DatabaseDropped,

    // Class 58 — System Error
    /// **Code `58000`**: `system_error`
    SystemError,
    /// **Code `58030`**: `io_error`
    IoError,
    /// **Code `58P01`**: `undefined_file`
    UndefinedFile,
    /// **Code `58P02`**: `duplicate_file`
    DuplicateFile,

    // Class 72 — Snapshot Failure
    /// **Code `72000`**: `snapshot_too_old`
    SnapshotTooOld,

    // Class F0 — Configuration File Error
    /// **Code `F0000`**: `config_file_error`
    ConfigFileError,
    /// **Code `F0001`**: `lock_file_exists`
    LockFileExists,

    // Class HV — Foreign Data Wrapper Error (SQL/MED)
    /// **Code `HV000`**: `fdw_error`
    FdwError,
    /// **Code `HV005`**: `fdw_column_name_not_found`
    FdwColumnNameNotFound,
    /// **Code `HV002`**: `fdw_dynamic_parameter_value_needed`
    FdwDynamicParameterValueNeeded,
    /// **Code `HV010`**: `fdw_function_sequence_error`
    FdwFunctionSequenceError,
    /// **Code `HV021`**: `fdw_inconsistent_descriptor_information`
    FdwInconsistentDescriptorInformation,
    /// **Code `HV024`**: `fdw_invalid_attribute_value`
    FdwInvalidAttributeValue,
    /// **Code `HV007`**: `fdw_invalid_column_name`
    FdwInvalidColumnName,
    /// **Code `HV008`**: `fdw_invalid_column_number`
    FdwInvalidColumnNumber,
    /// **Code `HV004`**: `fdw_invalid_data_type`
    FdwInvalidDataType,
    /// **Code `HV006`**: `fdw_invalid_data_type_descriptors`
    FdwInvalidDataTypeDescriptors,
    /// **Code `HV091`**: `fdw_invalid_descriptor_field_identifier`
    FdwInvalidDescriptorFieldIdentifier,
    /// **Code `HV00B`**: `fdw_invalid_handle`
    FdwInvalidHandle,
    /// **Code `HV00C`**: `fdw_invalid_option_index`
    FdwInvalidOptionIndex,
    /// **Code `HV00D`**: `fdw_invalid_option_name`
    FdwInvalidOptionName,
    /// **Code `HV090`**: `fdw_invalid_string_length_or_buffer_length`
    FdwInvalidStringLengthOrBufferLength,
    /// **Code `HV00A`**: `fdw_invalid_string_format`
    FdwInvalidStringFormat,
    /// **Code `HV009`**: `fdw_invalid_use_of_null_pointer`
    FdwInvalidUseOfNullPointer,
    /// **Code `HV014`**: `fdw_too_many_handles`
    FdwTooManyHandles,
    /// **Code `HV001`**: `fdw_out_of_memory`
    OutOfMemoryHV001,
    /// **Code `HV00P`**: `fdw_no_schemas`
    FdwNoSchemas,
    /// **Code `HV00J`**: `fdw_option_name_not_found`
    FdwOptionNameNotFound,
    /// **Code `HV00K`**: `fdw_reply_handle`
    FdwReplyHandle,
    /// **Code `HV00Q`**: `fdw_schema_not_found`
    FdwSchemaNotFound,
    /// **Code `HV00R`**: `fdw_table_not_found`
    FdwTableNotFound,
    /// **Code `HV00L`**: `fdw_unable_to_create_execution`
    FdwUnableToCreateExecution,
    /// **Code `HV00M`**: `fdw_unable_to_create_reply`
    FdwUnableToCreateReply,
    /// **Code `HV00N`**: `fdw_unable_to_establish_connection`
    FdwUnableToEstablishConnection,

    // Class P0 — PL/pgSQL Error
    /// **Code `P0000`**: `plpgsql_error`
    PlpgsqlError,
    /// **Code `P0001`**: `raise_exception`
    RaiseException,
    /// **Code `P0002`**: `no_data_found`
    NoDataFound,
    /// **Code `P0003`**: `too_many_rows`
    TooManyRows,
    /// **Code `P0004`**: `assert_failure`
    AssertFailure,

    // Class XX — Internal Error
    /// **Code `XX000`**: `internal_error`
    InternalError,
    /// **Code `XX001`**: `data_corrupted`
    DataCorrupted,
    /// **Code `XX002`**: `index_corrupted`
    IndexCorrupted,
}

impl PostgresErrorCode {
    /// Returns true if the error code is a success.
    pub fn is_success(self) -> bool {
        self == PostgresErrorCode::SuccessfulCompletion
    }

    /// Returns true if the error code is a warning.
    pub fn is_warning(self) -> bool {
        self >= PostgresErrorCode::Warning && self < PostgresErrorCode::NoData
    }

    /// Returns true if the error code is an error.
    pub fn is_error(self) -> bool {
        self >= PostgresErrorCode::NoData
    }

    /// Returns the static five-character SQLSTATE string for the given error code.
    pub const fn sqlstate(self) -> &'static str {
        match self {
            // Class 00
            PostgresErrorCode::SuccessfulCompletion => "00000",
            // Class 01
            PostgresErrorCode::Warning => "01000",
            PostgresErrorCode::DynamicResultSetsReturned => "0100C",
            PostgresErrorCode::ImplicitZeroBitPadding => "01008",
            PostgresErrorCode::NullValueEliminatedInSetFunction => "01003",
            PostgresErrorCode::PrivilegeNotGranted => "01007",
            PostgresErrorCode::PrivilegeNotRevoked => "01006",
            PostgresErrorCode::StringDataRightTruncation01004 => "01004",
            PostgresErrorCode::DeprecatedFeature => "01P01",
            // Class 02
            PostgresErrorCode::NoData => "02000",
            PostgresErrorCode::NoAdditionalDynamicResultSetsReturned => "02001",
            // Class 03
            PostgresErrorCode::SqlStatementNotYetComplete => "03000",
            // Class 08
            PostgresErrorCode::ConnectionException => "08000",
            PostgresErrorCode::ConnectionDoesNotExist => "08003",
            PostgresErrorCode::ConnectionFailure => "08006",
            PostgresErrorCode::SqlclientUnableToEstablishSqlconnection => "08001",
            PostgresErrorCode::SqlserverRejectedEstablishmentOfSqlconnection => "08004",
            PostgresErrorCode::TransactionResolutionUnknown => "08007",
            PostgresErrorCode::ProtocolViolation => "08P01",
            // Class 09
            PostgresErrorCode::TriggeredActionException => "09000",
            // Class 0A
            PostgresErrorCode::FeatureNotSupported => "0A000",
            // Class 0B
            PostgresErrorCode::InvalidTransactionInitiation => "0B000",
            // Class 0F
            PostgresErrorCode::LocatorException => "0F000",
            PostgresErrorCode::InvalidLocatorSpecification => "0F001",
            // Class 0L
            PostgresErrorCode::InvalidGrantor => "0L000",
            PostgresErrorCode::InvalidGrantOperation => "0LP01",
            // Class 0P
            PostgresErrorCode::InvalidRoleSpecification => "0P000",
            // Class 0Z
            PostgresErrorCode::DiagnosticsException => "0Z000",
            PostgresErrorCode::StackedDiagnosticsAccessedWithoutActiveHandler => "0Z002",
            // Class 20
            PostgresErrorCode::CaseNotFound => "20000",
            // Class 21
            PostgresErrorCode::CardinalityViolation => "21000",
            // Class 22
            PostgresErrorCode::DataException => "22000",
            PostgresErrorCode::ArraySubscriptError => "2202E",
            PostgresErrorCode::CharacterNotInRepertoire => "22021",
            PostgresErrorCode::DatetimeFieldOverflow => "22008",
            PostgresErrorCode::DivisionByZero => "22012",
            PostgresErrorCode::ErrorInAssignment => "22005",
            PostgresErrorCode::EscapeCharacterConflict => "2200B",
            PostgresErrorCode::IndicatorOverflow => "22022",
            PostgresErrorCode::IntervalFieldOverflow => "22015",
            PostgresErrorCode::InvalidArgumentForLogarithm => "2201E",
            PostgresErrorCode::InvalidArgumentForNtileFunction => "22014",
            PostgresErrorCode::InvalidArgumentForNthValueFunction => "22016",
            PostgresErrorCode::InvalidArgumentForPowerFunction => "2201F",
            PostgresErrorCode::InvalidArgumentForWidthBucketFunction => "2201G",
            PostgresErrorCode::InvalidCharacterValueForCast => "22018",
            PostgresErrorCode::InvalidDatetimeFormat => "22007",
            PostgresErrorCode::InvalidEscapeCharacter => "22019",
            PostgresErrorCode::InvalidEscapeOctet => "2200D",
            PostgresErrorCode::InvalidEscapeSequence => "22025",
            PostgresErrorCode::NonstandardUseOfEscapeCharacter => "22P06",
            PostgresErrorCode::InvalidIndicatorParameterValue => "22010",
            PostgresErrorCode::InvalidParameterValue => "22023",
            PostgresErrorCode::InvalidPrecedingOrFollowingSize => "22013",
            PostgresErrorCode::InvalidRegularExpression => "2201B",
            PostgresErrorCode::InvalidRowCountInLimitClause => "2201W",
            PostgresErrorCode::InvalidRowCountInResultOffsetClause => "2201X",
            PostgresErrorCode::InvalidTablesampleArgument => "2202H",
            PostgresErrorCode::InvalidTablesampleRepeat => "2202G",
            PostgresErrorCode::InvalidTimeZoneDisplacementValue => "22009",
            PostgresErrorCode::InvalidUseOfEscapeCharacter => "2200C",
            PostgresErrorCode::MostSpecificTypeMismatch => "2200G",
            PostgresErrorCode::NullValueNotAllowed22004 => "22004",
            PostgresErrorCode::NullValueNoIndicatorParameter => "22002",
            PostgresErrorCode::NumericValueOutOfRange => "22003",
            PostgresErrorCode::SequenceGeneratorLimitExceeded => "2200H",
            PostgresErrorCode::StringDataLengthMismatch => "22026",
            PostgresErrorCode::StringDataRightTruncation22001 => "22001",
            PostgresErrorCode::SubstringError => "22011",
            PostgresErrorCode::TrimError => "22027",
            PostgresErrorCode::UnterminatedCString => "22024",
            PostgresErrorCode::ZeroLengthCharacterString => "2200F",
            PostgresErrorCode::FloatingPointException => "22P01",
            PostgresErrorCode::InvalidTextRepresentation => "22P02",
            PostgresErrorCode::InvalidBinaryRepresentation => "22P03",
            PostgresErrorCode::BadCopyFileFormat => "22P04",
            PostgresErrorCode::UntranslatableCharacter => "22P05",
            PostgresErrorCode::NotAnXmlDocument => "2200L",
            PostgresErrorCode::InvalidXmlDocument => "2200M",
            PostgresErrorCode::InvalidXmlContent => "2200N",
            PostgresErrorCode::InvalidXmlComment => "2200S",
            PostgresErrorCode::InvalidXmlProcessingInstruction => "2200T",
            PostgresErrorCode::DuplicateJsonObjectKeyValue => "22030",
            PostgresErrorCode::InvalidArgumentForSqlJsonDatetimeFunction => "22031",
            PostgresErrorCode::InvalidJsonText => "22032",
            PostgresErrorCode::InvalidSqlJsonSubscript => "22033",
            PostgresErrorCode::MoreThanOneSqlJsonItem => "22034",
            PostgresErrorCode::NoSqlJsonItem => "22035",
            PostgresErrorCode::NonNumericSqlJsonItem => "22036",
            PostgresErrorCode::NonUniqueKeysInAJsonObject => "22037",
            PostgresErrorCode::SingletonSqlJsonItemRequired => "22038",
            PostgresErrorCode::SqlJsonArrayNotFound => "22039",
            PostgresErrorCode::SqlJsonMemberNotFound => "2203A",
            PostgresErrorCode::SqlJsonNumberNotFound => "2203B",
            PostgresErrorCode::SqlJsonObjectNotFound => "2203C",
            PostgresErrorCode::TooManyJsonArrayElements => "2203D",
            PostgresErrorCode::TooManyJsonObjectMembers => "2203E",
            PostgresErrorCode::SqlJsonScalarRequired => "2203F",
            // Class 23
            PostgresErrorCode::IntegrityConstraintViolation => "23000",
            PostgresErrorCode::RestrictViolation => "23001",
            PostgresErrorCode::NotNullViolation => "23502",
            PostgresErrorCode::ForeignKeyViolation => "23503",
            PostgresErrorCode::UniqueViolation => "23505",
            PostgresErrorCode::CheckViolation => "23514",
            PostgresErrorCode::ExclusionViolation => "23P01",
            // Class 24
            PostgresErrorCode::InvalidCursorState => "24000",
            // Class 25
            PostgresErrorCode::InvalidTransactionState => "25000",
            PostgresErrorCode::ActiveSqlTransaction => "25001",
            PostgresErrorCode::BranchTransactionAlreadyActive => "25002",
            PostgresErrorCode::HeldCursorRequiresSameIsolationLevel => "25008",
            PostgresErrorCode::InappropriateAccessModeForBranchTransaction => "25003",
            PostgresErrorCode::InappropriateIsolationLevelForBranchTransaction => "25004",
            PostgresErrorCode::NoActiveSqlTransactionForBranchTransaction => "25005",
            PostgresErrorCode::ReadOnlySqlTransaction => "25006",
            PostgresErrorCode::SchemaAndDataStatementMixingNotSupported => "25007",
            PostgresErrorCode::NoActiveSqlTransaction => "25P01",
            PostgresErrorCode::InFailedSqlTransaction => "25P02",
            PostgresErrorCode::IdleInTransactionSessionTimeout => "25P03",
            // Class 26
            PostgresErrorCode::InvalidSqlStatementName => "26000",
            // Class 27
            PostgresErrorCode::TriggeredDataChangeViolation => "27000",
            // Class 28
            PostgresErrorCode::InvalidAuthorizationSpecification => "28000",
            PostgresErrorCode::InvalidPassword => "28P01",
            // Class 2B
            PostgresErrorCode::DependentPrivilegeDescriptorsStillExist => "2B000",
            PostgresErrorCode::DependentObjectsStillExist => "2BP01",
            // Class 2D
            PostgresErrorCode::InvalidTransactionTermination => "2D000",
            // Class 2F
            PostgresErrorCode::SqlRoutineException => "2F000",
            PostgresErrorCode::FunctionExecutedNoReturnStatement => "2F005",
            PostgresErrorCode::ModifyingSqlDataNotPermitted2F002 => "2F002",
            PostgresErrorCode::ProhibitedSqlStatementAttempted2F003 => "2F003",
            PostgresErrorCode::ReadingSqlDataNotPermitted2F004 => "2F004",
            // Class 34
            PostgresErrorCode::InvalidCursorName => "34000",
            // Class 38
            PostgresErrorCode::ExternalRoutineException => "38000",
            PostgresErrorCode::ContainingSqlNotPermitted => "38001",
            PostgresErrorCode::ModifyingSqlDataNotPermitted38002 => "38002",
            PostgresErrorCode::ProhibitedSqlStatementAttempted38003 => "38003",
            PostgresErrorCode::ReadingSqlDataNotPermitted38004 => "38004",
            // Class 39
            PostgresErrorCode::ExternalRoutineInvocationException => "39000",
            PostgresErrorCode::InvalidSqlstateReturned => "39001",
            PostgresErrorCode::NullValueNotAllowed39004 => "39004",
            PostgresErrorCode::TriggerProtocolViolated => "39P01",
            PostgresErrorCode::SrfProtocolViolated => "39P02",
            PostgresErrorCode::EventTriggerProtocolViolated => "39P03",
            // Class 3B
            PostgresErrorCode::SavepointException => "3B000",
            PostgresErrorCode::InvalidSavepointSpecification => "3B001",
            // Class 3D
            PostgresErrorCode::InvalidCatalogName => "3D000",
            // Class 3F
            PostgresErrorCode::InvalidSchemaName => "3F000",
            // Class 40
            PostgresErrorCode::TransactionRollback => "40000",
            PostgresErrorCode::TransactionIntegrityConstraintViolation => "40002",
            PostgresErrorCode::SerializationFailure => "40001",
            PostgresErrorCode::StatementCompletionUnknown => "40003",
            PostgresErrorCode::DeadlockDetected => "40P01",
            // Class 42
            PostgresErrorCode::SyntaxErrorOrAccessRuleViolation => "42000",
            PostgresErrorCode::SyntaxError => "42601",
            PostgresErrorCode::InsufficientPrivilege => "42501",
            PostgresErrorCode::CannotCoerce => "42846",
            PostgresErrorCode::GroupingError => "42803",
            PostgresErrorCode::WindowingError => "42P20",
            PostgresErrorCode::InvalidRecursion => "42P19",
            PostgresErrorCode::InvalidForeignKey => "42830",
            PostgresErrorCode::InvalidName => "42602",
            PostgresErrorCode::NameTooLong => "42622",
            PostgresErrorCode::ReservedName => "42939",
            PostgresErrorCode::DatatypeMismatch => "42804",
            PostgresErrorCode::IndeterminateDatatype => "42P18",
            PostgresErrorCode::CollationMismatch => "42P21",
            PostgresErrorCode::IndeterminateCollation => "42P22",
            PostgresErrorCode::WrongObjectType => "42809",
            PostgresErrorCode::GeneratedAlways => "428C9",
            PostgresErrorCode::UndefinedColumn => "42703",
            PostgresErrorCode::UndefinedFunction => "42883",
            PostgresErrorCode::UndefinedTable => "42P01",
            PostgresErrorCode::UndefinedParameter => "42P02",
            PostgresErrorCode::UndefinedObject => "42704",
            PostgresErrorCode::DuplicateColumn => "42701",
            PostgresErrorCode::DuplicateCursor => "42P03",
            PostgresErrorCode::DuplicateDatabase => "42P04",
            PostgresErrorCode::DuplicateFunction => "42723",
            PostgresErrorCode::DuplicatePreparedStatement => "42P05",
            PostgresErrorCode::DuplicateSchema => "42P06",
            PostgresErrorCode::DuplicateTable => "42P07",
            PostgresErrorCode::DuplicateAlias => "42712",
            PostgresErrorCode::DuplicateObject => "42710",
            PostgresErrorCode::AmbiguousColumn => "42702",
            PostgresErrorCode::AmbiguousFunction => "42725",
            PostgresErrorCode::AmbiguousParameter => "42P08",
            PostgresErrorCode::AmbiguousAlias => "42P09",
            PostgresErrorCode::InvalidColumnReference => "42P10",
            PostgresErrorCode::InvalidColumnDefinition => "42611",
            PostgresErrorCode::InvalidCursorDefinition => "42P11",
            PostgresErrorCode::InvalidDatabaseDefinition => "42P12",
            PostgresErrorCode::InvalidFunctionDefinition => "42P13",
            PostgresErrorCode::InvalidPreparedStatementDefinition => "42P14",
            PostgresErrorCode::InvalidSchemaDefinition => "42P15",
            PostgresErrorCode::InvalidTableDefinition => "42P16",
            PostgresErrorCode::InvalidObjectDefinition => "42P17",
            // Class 44
            PostgresErrorCode::WithCheckOptionViolation => "44000",
            // Class 53
            PostgresErrorCode::InsufficientResources => "53000",
            PostgresErrorCode::DiskFull => "53100",
            PostgresErrorCode::OutOfMemory53200 => "53200",
            PostgresErrorCode::TooManyConnections => "53300",
            PostgresErrorCode::ConfigurationLimitExceeded => "53400",
            // Class 54
            PostgresErrorCode::ProgramLimitExceeded => "54000",
            PostgresErrorCode::StatementTooComplex => "54001",
            PostgresErrorCode::TooManyColumns => "54011",
            PostgresErrorCode::TooManyArguments => "54023",
            // Class 55
            PostgresErrorCode::ObjectNotInPrerequisiteState => "55000",
            PostgresErrorCode::ObjectInUse => "55006",
            PostgresErrorCode::CantChangeRuntimeParam => "55P02",
            PostgresErrorCode::LockNotAvailable => "55P03",
            PostgresErrorCode::UnsafeNewEnumValueUsage => "55P04",
            // Class 57
            PostgresErrorCode::OperatorIntervention => "57000",
            PostgresErrorCode::QueryCanceled => "57014",
            PostgresErrorCode::AdminShutdown => "57P01",
            PostgresErrorCode::CrashShutdown => "57P02",
            PostgresErrorCode::CannotConnectNow => "57P03",
            PostgresErrorCode::DatabaseDropped => "57P04",
            // Class 58
            PostgresErrorCode::SystemError => "58000",
            PostgresErrorCode::IoError => "58030",
            PostgresErrorCode::UndefinedFile => "58P01",
            PostgresErrorCode::DuplicateFile => "58P02",
            // Class 72
            PostgresErrorCode::SnapshotTooOld => "72000",
            // Class F0
            PostgresErrorCode::ConfigFileError => "F0000",
            PostgresErrorCode::LockFileExists => "F0001",
            // Class HV
            PostgresErrorCode::FdwError => "HV000",
            PostgresErrorCode::FdwColumnNameNotFound => "HV005",
            PostgresErrorCode::FdwDynamicParameterValueNeeded => "HV002",
            PostgresErrorCode::FdwFunctionSequenceError => "HV010",
            PostgresErrorCode::FdwInconsistentDescriptorInformation => "HV021",
            PostgresErrorCode::FdwInvalidAttributeValue => "HV024",
            PostgresErrorCode::FdwInvalidColumnName => "HV007",
            PostgresErrorCode::FdwInvalidColumnNumber => "HV008",
            PostgresErrorCode::FdwInvalidDataType => "HV004",
            PostgresErrorCode::FdwInvalidDataTypeDescriptors => "HV006",
            PostgresErrorCode::FdwInvalidDescriptorFieldIdentifier => "HV091",
            PostgresErrorCode::FdwInvalidHandle => "HV00B",
            PostgresErrorCode::FdwInvalidOptionIndex => "HV00C",
            PostgresErrorCode::FdwInvalidOptionName => "HV00D",
            PostgresErrorCode::FdwInvalidStringLengthOrBufferLength => "HV090",
            PostgresErrorCode::FdwInvalidStringFormat => "HV00A",
            PostgresErrorCode::FdwInvalidUseOfNullPointer => "HV009",
            PostgresErrorCode::FdwTooManyHandles => "HV014",
            PostgresErrorCode::OutOfMemoryHV001 => "HV001",
            PostgresErrorCode::FdwNoSchemas => "HV00P",
            PostgresErrorCode::FdwOptionNameNotFound => "HV00J",
            PostgresErrorCode::FdwReplyHandle => "HV00K",
            PostgresErrorCode::FdwSchemaNotFound => "HV00Q",
            PostgresErrorCode::FdwTableNotFound => "HV00R",
            PostgresErrorCode::FdwUnableToCreateExecution => "HV00L",
            PostgresErrorCode::FdwUnableToCreateReply => "HV00M",
            PostgresErrorCode::FdwUnableToEstablishConnection => "HV00N",
            // Class P0
            PostgresErrorCode::PlpgsqlError => "P0000",
            PostgresErrorCode::RaiseException => "P0001",
            PostgresErrorCode::NoDataFound => "P0002",
            PostgresErrorCode::TooManyRows => "P0003",
            PostgresErrorCode::AssertFailure => "P0004",
            // Class XX
            PostgresErrorCode::InternalError => "XX000",
            PostgresErrorCode::DataCorrupted => "XX001",
            PostgresErrorCode::IndexCorrupted => "XX002",
        }
    }
}
