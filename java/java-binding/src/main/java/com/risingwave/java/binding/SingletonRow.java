/*
 * Copyright 2026 RisingWave Labs
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

package com.risingwave.java.binding;

import com.risingwave.proto.Data.DataType.TypeName;
import java.util.ArrayList;
import java.util.List;

/**
 * A single Rust-owned row passed to Java by pointer. Rust keeps the row alive for the duration of
 * the JNI call and releases it afterwards, so Java must not keep this object after the call.
 */
public class SingletonRow extends BaseRow {
    public SingletonRow(long pointer) {
        super(pointer);
    }

    /**
     * Reads each value with the getter matching its type, where {@code types} holds one type per
     * row position. A NULL value is returned as {@code null}.
     */
    public List<Object> getValues(List<TypeName> types) {
        var values = new ArrayList<Object>(types.size());
        for (int index = 0; index < types.size(); index++) {
            values.add(isNull(index) ? null : getValue(index, types.get(index)));
        }
        return values;
    }

    private Object getValue(int index, TypeName type) {
        return switch (type) {
            case INT16 -> getShort(index);
            case INT32 -> getInt(index);
            case INT64 -> getLong(index);
            case FLOAT -> getFloat(index);
            case DOUBLE -> getDouble(index);
            case DECIMAL -> getDecimal(index);
            case BOOLEAN -> getBoolean(index);
            case DATE -> getDate(index);
            case TIME -> getTime(index);
            case TIMESTAMP -> getTimestamp(index);
            case TIMESTAMPTZ -> getTimestamptz(index);
            case INTERVAL -> getInterval(index);
            case VARCHAR -> getString(index);
            case JSONB -> getJsonb(index);
            case BYTEA -> getBytea(index);
            default -> throw new IllegalArgumentException("Unsupported row value type: " + type);
        };
    }
}
