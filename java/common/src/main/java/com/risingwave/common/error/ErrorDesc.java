package com.risingwave.common.error;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Strings;

public class ErrorDesc {
  private final ErrorCode errorCode;
  private final Object[] arguments;

  public ErrorDesc(ErrorCode errorCode, Object... arguments) {
    this.errorCode = requireNonNull(errorCode, "Error code can't be null!");
    this.arguments = arguments;
  }

  public int getCode() {
    return errorCode.getCode();
  }

  public String getMessage() {
    return Strings.lenientFormat(errorCode.getMessageTemplate(), arguments);
  }
}
