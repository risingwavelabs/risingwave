package com.risingwave.pgwire.msg;

public enum StatementType {
  INSERT,
  DELETE,
  UPDATE,
  SELECT,
  MOVE,
  FETCH,
  COPY,
  EXPLAIN,
  CREATE_TABLE,
  CREATE_MATERIALIZED_VIEW,
  CREATE_STREAM,
  DROP_TABLE,
  OTHER
}
