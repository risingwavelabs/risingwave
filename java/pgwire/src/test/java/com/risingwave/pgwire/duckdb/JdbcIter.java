package com.risingwave.pgwire.duckdb;

import com.risingwave.pgwire.database.PgErrorCode;
import com.risingwave.pgwire.database.PgException;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.database.TypeOid;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.pgwire.types.Values;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

class JdbcIter implements PgResult.PgIter {
  ResultSet res;

  JdbcIter(ResultSet rs) {
    // A ResultSet cursor is initially positioned before the first row;
    res = rs;
  }

  @NotNull
  @Override
  public List<PgFieldDescriptor> getRowDesc() throws PgException {
    try {
      ResultSetMetaData metaData = res.getMetaData();
      List<PgFieldDescriptor> rowDesc = new ArrayList<>(metaData.getColumnCount());
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        // In jdbc, column idx starts from 1.
        rowDesc.add(getFieldDesc(metaData, i));
      }
      return rowDesc;
    } catch (Exception exp) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, exp);
    }
  }

  private PgFieldDescriptor getFieldDesc(ResultSetMetaData meta, int i) throws Exception {
    TypeOid colType = mapJdbcTypeToTypeOid(meta.getColumnType(i));
    return new PgFieldDescriptor(meta.getColumnName(i), colType);
  }

  private static TypeOid mapJdbcTypeToTypeOid(int i) throws PgException {
    switch (i) {
      case Types.BOOLEAN:
        return TypeOid.BOOLEAN;
      case Types.BIGINT:
        return TypeOid.BIGINT;
      case Types.SMALLINT:
        return TypeOid.SMALLINT;
      case Types.INTEGER:
        return TypeOid.INT;
      case Types.FLOAT:
        return TypeOid.FLOAT4;
      case Types.REAL:
      case Types.DOUBLE:
        return TypeOid.FLOAT8;
      case Types.CHAR:
        return TypeOid.CHAR_ARRAY;
      case Types.VARCHAR:
        return TypeOid.VARCHAR;
      case Types.DATE:
        return TypeOid.DATE;
      case Types.TIMESTAMP:
        return TypeOid.TIMESTAMP;
      default:
        throw new PgException(PgErrorCode.FEATURE_NOT_SUPPORTED, "unrecognized jdbc type: %d", i);
    }
  }

  @Override
  public boolean next() throws PgException {
    try {
      return res.next();
    } catch (SQLException exp) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, exp);
    }
  }

  @Override
  public List<PgValue> getRow() throws PgException {
    try {
      int colNum = res.getMetaData().getColumnCount();
      List<PgValue> ret = new ArrayList<>(colNum);
      for (int i = 1; i <= colNum; i++) {
        // In jdbc, column idx starts from 1.
        ret.add(getValue(i, res));
      }
      return ret;
    } catch (SQLException exp) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, exp);
    }
  }

  public static PgValue getValue(int i, ResultSet res) throws SQLException, PgException {
    switch (res.getMetaData().getColumnType(i)) {
      case Types.BOOLEAN:
        return Values.createBoolean(res.getBoolean(i));
      case Types.BIGINT:
        return Values.createBigInt(res.getLong(i));
      case Types.SMALLINT:
        return Values.createSmallInt(res.getShort(i));
      case Types.INTEGER:
        return Values.createInt(res.getInt(i));
      case Types.FLOAT:
        return Values.createFloat(res.getFloat(i));
      case Types.REAL:
      case Types.DOUBLE:
        return Values.createDouble(res.getDouble(i));
      case Types.CHAR:
      case Types.VARCHAR:
        return Values.createString(res.getString(i));
      case Types.DATE:
        return Values.createDate(res.getDate(i));
      case Types.TIMESTAMP:
        return Values.createTimestamp(res.getTimestamp(i));
      default:
        throw new PgException(PgErrorCode.FEATURE_NOT_SUPPORTED, "unrecognized jdbc type: %d", i);
    }
  }
}
