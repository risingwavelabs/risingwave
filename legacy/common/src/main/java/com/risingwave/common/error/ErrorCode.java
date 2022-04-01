package com.risingwave.common.error;

public interface ErrorCode {
  int getCode();

  String getMessageTemplate();
}
