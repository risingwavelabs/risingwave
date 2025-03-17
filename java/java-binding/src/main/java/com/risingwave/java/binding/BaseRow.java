// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.java.binding;

public class BaseRow {
    protected final long pointer;

    protected BaseRow(long pointer) {
        this.pointer = pointer;
    }

    public boolean isNull(int index) {
        return Binding.iteratorIsNull(pointer, index);
    }

    public short getShort(int index) {
        return Binding.iteratorGetInt16Value(pointer, index);
    }

    public int getInt(int index) {
        return Binding.iteratorGetInt32Value(pointer, index);
    }

    public long getLong(int index) {
        return Binding.iteratorGetInt64Value(pointer, index);
    }

    public float getFloat(int index) {
        return Binding.iteratorGetFloatValue(pointer, index);
    }

    public double getDouble(int index) {
        return Binding.iteratorGetDoubleValue(pointer, index);
    }

    public boolean getBoolean(int index) {
        return Binding.iteratorGetBooleanValue(pointer, index);
    }

    public String getString(int index) {
        return Binding.iteratorGetStringValue(pointer, index);
    }

    public java.time.LocalDateTime getTimestamp(int index) {
        return Binding.iteratorGetTimestampValue(pointer, index);
    }

    public java.time.OffsetDateTime getTimestamptz(int index) {
        return Binding.iteratorGetTimestamptzValue(pointer, index);
    }

    public java.time.LocalTime getTime(int index) {
        return Binding.iteratorGetTimeValue(pointer, index);
    }

    public java.math.BigDecimal getDecimal(int index) {
        return Binding.iteratorGetDecimalValue(pointer, index);
    }

    public java.time.LocalDate getDate(int index) {
        return Binding.iteratorGetDateValue(pointer, index);
    }

    // string representation of interval: "2 mons 3 days 00:00:00.000004" or "P1Y2M3DT4H5M6.789123S"
    public String getInterval(int index) {
        return Binding.iteratorGetIntervalValue(pointer, index);
    }

    // string representation of jsonb: '{"key": "value"}'
    public String getJsonb(int index) {
        return Binding.iteratorGetJsonbValue(pointer, index);
    }

    public byte[] getBytea(int index) {
        return Binding.iteratorGetByteaValue(pointer, index);
    }

    /**
     * Only supports one-dimensional array right now
     *
     * @return an Object[] which will be used in java.sql.Connection#createArrayOf(String typeName,
     *     Object[] elements)
     */
    public <T> Object[] getArray(int index, Class<T> clazz) {
        var val = Binding.iteratorGetArrayValue(pointer, index, clazz);
        assert (val instanceof Object[]);
        return (Object[]) val;
    }
}
