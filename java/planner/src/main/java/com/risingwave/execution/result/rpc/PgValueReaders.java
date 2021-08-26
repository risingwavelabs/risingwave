package com.risingwave.execution.result.rpc;

import static com.risingwave.execution.result.rpc.PrimitiveValueReader.createValueReader;

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
import com.risingwave.pgwire.types.Values;
import com.risingwave.proto.data.Column;
import java.io.InputStream;
import javax.annotation.Nullable;

public class PgValueReaders {
  public static PgValueReader create(Any column) {
    try {
      if (column.is(Column.class)) {
        return createPrimitiveReader(column.unpack(Column.class));
      }

      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Unsupported column type: %s", column.getTypeUrl());
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }

  private static PgValueReader createPrimitiveReader(Column column) {
    BooleanBufferReader nullBitmap = getNullBitmap(column);
    // FIXME: currently the length of buffer size is 1 so directly get from it.
    //  Should fix here when handle other types like char/varchar.
    InputStream valuesStream = BufferReaders.decode(column.getValues(0));

    switch (column.getColumnType().getTypeName()) {
      case INT16:
        return createValueReader(
            Values::createSmallInt, new ShortBufferReader(valuesStream), nullBitmap);
      case INT32:
        return createValueReader(Values::createInt, new IntBufferReader(valuesStream), nullBitmap);
      case INT64:
        return createValueReader(
            Values::createBigInt, new LongBufferReader(valuesStream), nullBitmap);
      case FLOAT:
        return createValueReader(
            Values::createFloat, new FloatBufferReader(valuesStream), nullBitmap);
      case DOUBLE:
        return createValueReader(
            Values::createDouble, new DoubleBufferReader(valuesStream), nullBitmap);
      case BOOLEAN:
        return createValueReader(
            Values::createBoolean, new BooleanBufferReader(valuesStream), nullBitmap);
      case DATE:
        return createValueReader(Values::createDate, new IntBufferReader(valuesStream), nullBitmap);
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unsupported column type: %s", column.getColumnType());
    }
  }

  @Nullable
  protected static BooleanBufferReader getNullBitmap(Column columnCommon) {
    BooleanBufferReader nullBitmap = null;
    if (columnCommon.hasNullBitmap()) {
      nullBitmap = new BooleanBufferReader(BufferReaders.decode(columnCommon.getNullBitmap()));
    }

    return nullBitmap;
  }
}
