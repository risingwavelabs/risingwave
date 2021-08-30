package com.risingwave.pgwire.database;

public enum TypeOid {
  BOOLEAN(16),
  BIGINT(20),
  SMALLINT(21),
  INT(23),
  FLOAT4(700),
  FLOAT8(701),
  CHAR_ARRAY(1002),
  VARCHAR(1043),
  DATE(1082),
  TIMESTAMP(1114),
  DECIMAL(1231);

  TypeOid(int i) {
    this.id = i;
  }

  private final int id;

  public int asInt() {
    return id;
  }
}
