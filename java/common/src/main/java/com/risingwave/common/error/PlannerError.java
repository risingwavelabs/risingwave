package com.risingwave.common.error;

public enum PlannerError implements ErrorCode {
  INTERNAL(5001, "Internal planner error happens.");

  private final int code;
  private final String messageTemplate;

  PlannerError(int errorCode, String messageTemplate) {
    this.code = errorCode;
    this.messageTemplate = messageTemplate;
  }

  @Override
  public int getCode() {
    return code;
  }

  @Override
  public String getMessageTemplate() {
    return messageTemplate;
  }

  public static ErrorKind getErrorKind() {
    return ErrorKind.PLANNER;
  }
}
