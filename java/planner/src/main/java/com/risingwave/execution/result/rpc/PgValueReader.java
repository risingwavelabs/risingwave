package com.risingwave.execution.result.rpc;

import com.risingwave.pgwire.types.PgValue;
import javax.annotation.Nullable;

public interface PgValueReader {
  @Nullable
  PgValue next();
}
