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
  // Introduce ORDER_BY statement type cuz Calcite unvalidated AST has SqlKind.ORDER_BY. Note that
  // Statement Type is not designed to be one to one mapping with SqlKind.
  ORDER_BY,
  OTHER
}
