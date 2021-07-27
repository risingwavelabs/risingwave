package com.risingwave.pgwire.msg;

public enum StatementType {
  INSERT,
  DELETE,
  UPDATE,
  SELECT,
  MOVE,
  FETCH,
  COPY,
  OTHER
}
