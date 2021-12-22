package com.risingwave.pgwire.database;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;

/** FieldDescriptor in PG Protocol */
public class PgFieldDescriptor {
  public final String name;
  public final int tableOid;
  public final short colAttrNum;

  // NOTE: Static code for data type. To see the oid of a specific type in Postgres,
  // use the following command:
  //   SELECT oid FROM pg_type WHERE typname = 'int4';
  public final TypeOid typeOid;

  public final short typeLen;
  public final int typeModifier;
  public final short formatCode;

  public PgFieldDescriptor(String name, TypeOid toid) throws PgException {
    this.typeModifier = -1;
    this.name = name;
    this.formatCode = 0; /*text*/
    this.typeOid = toid;
    this.tableOid = 0;
    this.colAttrNum = 0; // Useful only when it's a struct/ref.

    switch (toid) {
      case BOOLEAN:
        this.typeLen = 1;
        break;
      case INT:
      case FLOAT4:
      case DATE:
        this.typeLen = 4;
        break;
      case BIGINT:
      case FLOAT8:
      case TIME:
      case TIMESTAMPZ:
      case TIMESTAMP:
        this.typeLen = 8;
        break;
      case SMALLINT:
        this.typeLen = 2;
        break;
      case CHAR_ARRAY:
      case VARCHAR:
      case DECIMAL:
        this.typeLen = -1; /*maxlength*/
        break;
      default:
        throw new PgException(
            PgErrorCode.FEATURE_NOT_SUPPORTED,
            "PgFieldDescriptor lack support of type %d",
            toid.asInt());
    }
  }
}
