// Copyright 2023 RisingWave Labs
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

public class BaseRow implements AutoCloseable {
    protected final long pointer;
    private boolean isClosed;

    protected BaseRow(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    public boolean isNull(int index) {
        return Binding.rowIsNull(pointer, index);
    }

    public short getShort(int index) {
        return Binding.rowGetInt16Value(pointer, index);
    }

    public int getInt(int index) {
        return Binding.rowGetInt32Value(pointer, index);
    }

    public long getLong(int index) {
        return Binding.rowGetInt64Value(pointer, index);
    }

    public float getFloat(int index) {
        return Binding.rowGetFloatValue(pointer, index);
    }

    public double getDouble(int index) {
        return Binding.rowGetDoubleValue(pointer, index);
    }

    public boolean getBoolean(int index) {
        return Binding.rowGetBooleanValue(pointer, index);
    }

    public String getString(int index) {
        return Binding.rowGetStringValue(pointer, index);
    }

    public java.sql.Date getDateTime(int index) {
        return Binding.rowGetDateTimeValue(pointer, index);
    }

    public java.math.BigDecimal getDecimal(int index) {
        return Binding.rowGetDecimalValue(pointer, index);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.rowClose(pointer);
        }
    }
}
