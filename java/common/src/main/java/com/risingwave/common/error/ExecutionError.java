package com.risingwave.common.error;

public enum ExecutionError implements ErrorCode {
  INTERNAL(6000, "Internal planner error happens."),
  NOT_IMPLEMENTED(6001, "%s not supported yet!"),
  CURRENT_SCHEMA_NOT_SET(6002, "Current schema not set!");

  private final int errorCode;
  private final String errorTemplate;

  ExecutionError(int errorCode, String errorTemplate) {
    this.errorCode = errorCode;
    this.errorTemplate = errorTemplate;
  }

  @Override
  public int getCode() {
    return errorCode;
  }

  @Override
  public String getMessageTemplate() {
    return errorTemplate;
  }

  public static ErrorKind getErrorKind() {
    return ErrorKind.EXECUTION_ERROR;
  }
}
