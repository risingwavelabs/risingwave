package com.risingwave.pgwire.database;

import com.google.common.base.Preconditions;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/** Type OID in PostgresQL */
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
  TIME(1083),
  TIMESTAMP(1114),
  TIMESTAMPZ(1184),
  DECIMAL(1231);

  TypeOid(int i) {
    this.id = i;
  }

  private final int id;

  public int asInt() {
    return id;
  }

  public static TypeOid of(SqlTypeName t) {
    switch (t) {
      case BOOLEAN:
        return TypeOid.BOOLEAN;
      case SMALLINT:
        return TypeOid.SMALLINT;
      case INTEGER:
        return TypeOid.INT;
      case BIGINT:
        return TypeOid.BIGINT;
      case FLOAT:
        return TypeOid.FLOAT4;
      case DOUBLE:
        return TypeOid.FLOAT8;
      case CHAR:
        return TypeOid.CHAR_ARRAY;
      case DECIMAL:
        return TypeOid.DECIMAL;
      case DATE:
        return TypeOid.DATE;
      case TIME:
        return TypeOid.TIME;
      case TIMESTAMP:
        return TypeOid.TIMESTAMP;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TypeOid.TIMESTAMPZ;
      case VARCHAR:
        return TypeOid.VARCHAR;
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unsupported sql type: %s", t);
    }
  }

  public static TypeOid of(RelDataType t) {
    Preconditions.checkArgument(!t.isStruct());
    return of(t.getSqlTypeName());
  }
}
