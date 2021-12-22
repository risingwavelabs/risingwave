package com.risingwave.execution.result.rpc;

import com.google.protobuf.Any;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.result.rpc.primitive.BooleanBufferReader;
import com.risingwave.execution.result.rpc.primitive.BufferReaders;
import com.risingwave.execution.result.rpc.primitive.DoubleBufferReader;
import com.risingwave.execution.result.rpc.primitive.FloatBufferReader;
import com.risingwave.execution.result.rpc.primitive.IntBufferReader;
import com.risingwave.execution.result.rpc.primitive.LongBufferReader;
import com.risingwave.execution.result.rpc.primitive.ShortBufferReader;
import com.risingwave.execution.result.rpc.string.StringValueReader;
import com.risingwave.pgwire.database.TypeOid;
import com.risingwave.pgwire.types.Values;
import com.risingwave.proto.data.Buffer;
import com.risingwave.proto.data.Column;
import java.io.InputStream;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Postgres value readers
 */
public class PgValueReaders {
  public static PgValueReader create(Any column, TypeOid type) {
    try {
      if (column.is(Column.class)) {
        Column unpackedColumn = column.unpack(Column.class);

        switch (type) {
          case SMALLINT:
          case INT:
          case BIGINT:
          case FLOAT4:
          case FLOAT8:
          case BOOLEAN:
          case DATE:
          case TIME:
          case TIMESTAMP:
          case TIMESTAMPZ:
            return createPrimitiveReader(unpackedColumn, type);
          case CHAR_ARRAY:
          case VARCHAR:
          case DECIMAL:
            return createStringReader(unpackedColumn, type);
          default:
            break;
        }
      }

      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Unsupported column type: %s", column.getTypeUrl());
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }

  private static PgValueReader createStringReader(Column column, TypeOid type) {
    BooleanBufferReader nullBitmap = getNullBitmap(column);
    List<Buffer> bufferList = column.getArray().getValuesList();

    if (bufferList.size() < 2) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Column buffer illegal");
    }

    InputStream offsetStream = BufferReaders.decode(column.getArray().getValues(0));
    InputStream dataStream = BufferReaders.decode(column.getArray().getValues(1));

    switch (type) {
      case CHAR_ARRAY:
      case VARCHAR:
        return StringValueReader.createValueReader(
            Values::createString, new LongBufferReader(offsetStream), dataStream, nullBitmap);
      case DECIMAL:
        return StringValueReader.createValueReader(
            Values::createDecimal, new LongBufferReader(offsetStream), dataStream, nullBitmap);
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR,
            "Unsupported string column type: %s",
            type.name());
    }
  }

  private static PgValueReader createPrimitiveReader(Column column, TypeOid type) {
    BooleanBufferReader nullBitmap = getNullBitmap(column);
    // FIXME: currently the length of buffer size is 1 so directly get from it.
    //  Should fix here when handle other types like char/varchar.
    InputStream valuesStream = BufferReaders.decode(column.getArray().getValues(0));

    switch (type) {
      case SMALLINT:
        return PrimitiveValueReader.createValueReader(
            Values::createSmallInt, new ShortBufferReader(valuesStream), nullBitmap);
      case INT:
        return PrimitiveValueReader.createValueReader(
            Values::createInt, new IntBufferReader(valuesStream), nullBitmap);
      case BIGINT:
        return PrimitiveValueReader.createValueReader(
            Values::createBigInt, new LongBufferReader(valuesStream), nullBitmap);
      case FLOAT4:
        return PrimitiveValueReader.createValueReader(
            Values::createFloat, new FloatBufferReader(valuesStream), nullBitmap);
      case FLOAT8:
        return PrimitiveValueReader.createValueReader(
            Values::createDouble, new DoubleBufferReader(valuesStream), nullBitmap);
      case BOOLEAN:
        return PrimitiveValueReader.createValueReader(
            Values::createBoolean, new BooleanBufferReader(valuesStream), nullBitmap);
      case DATE:
        return PrimitiveValueReader.createValueReader(
            Values::createDate, new IntBufferReader(valuesStream), nullBitmap);
      case TIME:
        return PrimitiveValueReader.createValueReader(
            Values::createTime, new LongBufferReader(valuesStream), nullBitmap);
      case TIMESTAMP:
        return PrimitiveValueReader.createValueReader(
            Values::createTimestamp, new LongBufferReader(valuesStream), nullBitmap);
      case TIMESTAMPZ:
        return PrimitiveValueReader.createValueReader(
            Values::createTimestampz, new LongBufferReader(valuesStream), nullBitmap);
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unsupported column type: %s", type.name());
    }
  }

  @Nullable
  protected static BooleanBufferReader getNullBitmap(Column columnCommon) {
    BooleanBufferReader nullBitmap = null;
    if (columnCommon.getArray().hasNullBitmap()) {
      nullBitmap = new BooleanBufferReader(BufferReaders.decode(columnCommon.getArray().getNullBitmap()));
    }

    return nullBitmap;
  }
}
