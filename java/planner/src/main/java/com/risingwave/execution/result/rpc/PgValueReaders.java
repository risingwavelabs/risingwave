package com.risingwave.execution.result.rpc;

import static com.risingwave.execution.result.rpc.PrimitiveValueReader.createValueReader;

import com.google.protobuf.Any;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.result.rpc.primitive.BooleanBufferReader;
import com.risingwave.execution.result.rpc.primitive.BufferReaders;
import com.risingwave.execution.result.rpc.primitive.IntBufferReader;
import com.risingwave.execution.result.rpc.primitive.LongBufferReader;
import com.risingwave.pgwire.types.Values;
import com.risingwave.proto.data.ColumnCommon;
import com.risingwave.proto.data.FixedWidthNumericColumn;
import java.io.InputStream;
import javax.annotation.Nullable;

public class PgValueReaders {
  public static PgValueReader create(Any column) {
    try {
      if (column.is(FixedWidthNumericColumn.class)) {
        return createPrimitiveReader(column.unpack(FixedWidthNumericColumn.class));
      }

      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Unsupported column type: %s", column.getTypeUrl());
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }

  private static PgValueReader createPrimitiveReader(FixedWidthNumericColumn column) {
    ColumnCommon columnCommon = column.getCommonParts();
    BooleanBufferReader nullBitmap = getNullBitmap(columnCommon);
    InputStream valuesStream = BufferReaders.decode(column.getValues());

    switch (columnCommon.getColumnType()) {
      case INT32:
        return createValueReader(Values::createInt, new IntBufferReader(valuesStream), nullBitmap);
      case INT64:
        return createValueReader(
            Values::createBigInt, new LongBufferReader(valuesStream), nullBitmap);
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR,
            "Unsupported column type: %s",
            columnCommon.getColumnType());
    }
  }

  @Nullable
  protected static BooleanBufferReader getNullBitmap(ColumnCommon columnCommon) {
    BooleanBufferReader nullBitmap = null;
    if (columnCommon.hasNullBitmap()) {
      nullBitmap = new BooleanBufferReader(BufferReaders.decode(columnCommon.getNullBitmap()));
    }

    return nullBitmap;
  }
}
