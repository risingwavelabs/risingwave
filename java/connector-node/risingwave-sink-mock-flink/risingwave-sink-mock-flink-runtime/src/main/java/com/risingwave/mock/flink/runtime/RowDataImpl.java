/*
 * Copyright 2024 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.mock.flink.runtime;

import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;

/*
 * Converts data types from RW to Flink's RowData (Format of flink writes to sink).
 */
public class RowDataImpl implements RowData {

  private SinkRow sinkRow;
  private int arity;

  public RowDataImpl(SinkRow sinkRow, int arity) {
    this.sinkRow = sinkRow;
    this.arity = arity;
  }

  @Override
  public int getArity() {
    return arity;
  }

  @Override
  public RowKind getRowKind() {
    switch (sinkRow.getOp()) {
      case INSERT:
        return RowKind.INSERT;
      case UPDATE_INSERT:
        return RowKind.UPDATE_AFTER;
      case UPDATE_DELETE:
        return RowKind.UPDATE_BEFORE;
      case DELETE:
        return RowKind.DELETE;
      default:
        throw Status.INTERNAL
            .withDescription("Unknown operation: " + sinkRow.getOp())
            .asRuntimeException();
    }
  }

  @Override
  public void setRowKind(RowKind rowKind) {
    throw Status.INTERNAL.withDescription("Can't support set row kind").asRuntimeException();
  }

  @Override
  public boolean isNullAt(int i) {
    return sinkRow.get(i) == null;
  }

  @Override
  public boolean getBoolean(int i) {
    return (boolean) sinkRow.get(i);
  }

  @Override
  public byte getByte(int i) {
    return (byte) sinkRow.get(i);
  }

  @Override
  public short getShort(int i) {
    return (short) sinkRow.get(i);
  }

  @Override
  public int getInt(int i) {
    Object value = sinkRow.get(i);
    if (value instanceof LocalDate) {
      return (int) ((LocalDate) value).toEpochDay();
    } else if (value instanceof LocalTime) {
      // number of milliseconds of the day
      return (int) (((LocalTime) value).toNanoOfDay() / 1_000_000L);
    } else {
      return (int) value;
    }
  }

  @Override
  public long getLong(int i) {
    return (long) sinkRow.get(i);
  }

  @Override
  public float getFloat(int i) {
    return (float) sinkRow.get(i);
  }

  @Override
  public double getDouble(int i) {
    return (double) sinkRow.get(i);
  }

  @Override
  public StringData getString(int i) {
    return StringData.fromString((String) sinkRow.get(i));
  }

  @Override
  public DecimalData getDecimal(int i, int i1, int i2) {
    return DecimalData.fromBigDecimal((java.math.BigDecimal) sinkRow.get(i), i1, i2);
  }

  @Override
  public TimestampData getTimestamp(int i, int i1) {
    Object value = sinkRow.get(i);
    if (value instanceof LocalDateTime) {
      return TimestampData.fromLocalDateTime((LocalDateTime) value);
    } else if (value instanceof OffsetDateTime) {
      return TimestampData.fromInstant(((OffsetDateTime) value).toInstant());
    } else {
      throw Status.INTERNAL.withDescription("unreachable").asRuntimeException();
    }
  }

  @Override
  public <T> RawValueData<T> getRawValue(int i) {
    throw Status.INTERNAL.withDescription("Raw type is not supported yet\"").asRuntimeException();
  }

  @Override
  public byte[] getBinary(int i) {
    return Base64.getDecoder().decode((String) sinkRow.get(i));
  }

  @Override
  public ArrayData getArray(int i) {
    Object[] array = ((ArrayList<?>) sinkRow.get(i)).toArray();
    return new GenericArrayData(array);
  }

  @Override
  public MapData getMap(int i) {
    throw Status.INTERNAL.withDescription("Can't support Raw").asRuntimeException();
  }

  @Override
  public RowData getRow(int i, int i1) {
    throw new RuntimeException("STRUCT type is not supported yet");
  }
}
