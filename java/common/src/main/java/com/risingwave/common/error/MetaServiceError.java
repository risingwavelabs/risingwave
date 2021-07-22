package com.risingwave.common.error;

public enum MetaServiceError implements ErrorCode {
  DATABASE_ALREADY_EXISTS(1001, "Database %s already exists!"),
  SCHEMA_ALREADY_EXISTS(1002, "Schema %s already exists in %s!"),
  TABLE_ALREADY_EXISTS(1003, "Table %s already exists in %s!"),
  COLUMN_ALREADY_EXISTS(1004, "Column %s already exists in %s"),
  DATABASE_NOT_EXISTS(1005, "Database %s not exists!"),
  SCHEMA_NOT_EXISTS(1006, "Schema %s not exists!"),
  TABLE_NOT_EXISTS(1007, "Table %s not exists!");

  private final int errorCode;
  private final String messageTemplate;

  MetaServiceError(int errorCode, String messageTemplate) {
    this.errorCode = errorCode;
    this.messageTemplate = messageTemplate;
  }

  @Override
  public int getCode() {
    return errorCode;
  }

  @Override
  public String getMessageTemplate() {
    return messageTemplate;
  }

  public static ErrorKind getErrorKind() {
    return ErrorKind.META_SERVICE;
  }
}
