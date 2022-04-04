package com.risingwave.common.exception;

import com.risingwave.common.error.ErrorCode;
import com.risingwave.common.error.ErrorDesc;

// TODO: Deprecate this class
public class RisingWaveException extends RuntimeException {
  private ErrorDesc errorDesc;

  public RisingWaveException(ErrorDesc errorDesc, Throwable cause) {
    super(errorDesc.getMessage(), cause);
  }

  public RisingWaveException(ErrorDesc errorDesc) {
    super(errorDesc.getMessage());
  }

  public static RisingWaveException from(ErrorCode errorCode, Throwable cause) {
    return new RisingWaveException(new ErrorDesc(errorCode), cause);
  }

  public static RisingWaveException from(ErrorCode errorCode, Object... args) {
    return new RisingWaveException(new ErrorDesc(errorCode, args));
  }
}
