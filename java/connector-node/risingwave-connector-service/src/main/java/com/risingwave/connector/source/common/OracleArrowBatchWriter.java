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

package com.risingwave.connector.source.common;

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType.TypeName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Writes Oracle result rows into Arrow record batches whose types match RisingWave's Arrow
 * conversion, so Rust can turn each batch into a data chunk without a text codec.
 */
final class OracleArrowBatchWriter implements AutoCloseable {
    private static final String EXTENSION_NAME_KEY = "ARROW:extension:name";
    private static final String DECIMAL_EXTENSION = "arrowudf.decimal";
    private static final String JSON_EXTENSION = "arrowudf.json";
    private static final String UTC = "+00:00";

    private final VectorSchemaRoot root;
    private final List<ColumnWriter> writers = new ArrayList<>();
    private int rowCount;

    OracleArrowBatchWriter(BufferAllocator allocator, ConnectorServiceProto.TableSchema tableSchema)
            throws SQLException {
        var fields = new ArrayList<Field>();
        for (var column : tableSchema.getColumnsList()) {
            fields.add(arrowField(column.getName(), column.getColumnType().getTypeName()));
        }
        root = VectorSchemaRoot.create(new Schema(fields), allocator);
        root.allocateNew();
        for (int index = 0; index < fields.size(); index++) {
            var typeName = tableSchema.getColumns(index).getColumnType().getTypeName();
            writers.add(columnWriter(typeName, root.getVector(index), index + 1));
        }
    }

    void append(ResultSet result) throws SQLException {
        for (var writer : writers) {
            writer.write(result, rowCount);
        }
        rowCount++;
    }

    int rowCount() {
        return rowCount;
    }

    byte[] finishBatch() throws IOException {
        root.setRowCount(rowCount);
        var output = new ByteArrayOutputStream();
        try (var writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        root.getFieldVectors().forEach(FieldVector::reset);
        root.setRowCount(0);
        rowCount = 0;
        return output.toByteArray();
    }

    @Override
    public void close() {
        root.close();
    }

    private static Field arrowField(String name, TypeName typeName) throws SQLException {
        ArrowType type =
                switch (typeName) {
                    case INT16 -> new ArrowType.Int(16, true);
                    case INT32 -> new ArrowType.Int(32, true);
                    case INT64 -> new ArrowType.Int(64, true);
                    case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                    case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                    case BOOLEAN -> ArrowType.Bool.INSTANCE;
                    case DATE -> new ArrowType.Date(DateUnit.DAY);
                    case TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
                    case TIMESTAMPTZ -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, UTC);
                    case DECIMAL, VARCHAR, JSONB -> ArrowType.Utf8.INSTANCE;
                    case BYTEA -> ArrowType.Binary.INSTANCE;
                    default ->
                            throw new SQLException(
                                    "Unsupported RisingWave type for Oracle snapshot: " + typeName);
                };
        Map<String, String> metadata =
                switch (typeName) {
                    case DECIMAL -> Map.of(EXTENSION_NAME_KEY, DECIMAL_EXTENSION);
                    case JSONB -> Map.of(EXTENSION_NAME_KEY, JSON_EXTENSION);
                    default -> null;
                };
        return new Field(name, new FieldType(true, type, null, metadata), null);
    }

    private static ColumnWriter columnWriter(TypeName typeName, FieldVector vector, int column)
            throws SQLException {
        return switch (typeName) {
            case INT16 -> {
                var target = (SmallIntVector) vector;
                yield (result, row) -> {
                    var value = result.getShort(column);
                    if (result.wasNull()) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value);
                    }
                };
            }
            case INT32 -> {
                var target = (IntVector) vector;
                yield (result, row) -> {
                    var value = result.getInt(column);
                    if (result.wasNull()) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value);
                    }
                };
            }
            case INT64 -> {
                var target = (BigIntVector) vector;
                yield (result, row) -> {
                    var value = result.getLong(column);
                    if (result.wasNull()) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value);
                    }
                };
            }
            case FLOAT -> {
                var target = (Float4Vector) vector;
                yield (result, row) -> {
                    var value = result.getFloat(column);
                    if (result.wasNull()) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value);
                    }
                };
            }
            case DOUBLE -> {
                var target = (Float8Vector) vector;
                yield (result, row) -> {
                    var value = result.getDouble(column);
                    if (result.wasNull()) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value);
                    }
                };
            }
            case BOOLEAN -> {
                var target = (BitVector) vector;
                yield (result, row) -> {
                    var value = result.getBoolean(column);
                    if (result.wasNull()) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value ? 1 : 0);
                    }
                };
            }
            case DATE -> {
                var target = (DateDayVector) vector;
                yield (result, row) -> {
                    var value = result.getDate(column);
                    if (value == null) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, Math.toIntExact(value.toLocalDate().toEpochDay()));
                    }
                };
            }
            case TIMESTAMP -> {
                var target = (TimeStampMicroVector) vector;
                yield (result, row) -> {
                    var value = result.getTimestamp(column);
                    if (value == null) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, epochMicros(value.toLocalDateTime()));
                    }
                };
            }
            case TIMESTAMPTZ -> {
                var target = (TimeStampMicroTZVector) vector;
                yield (result, row) -> {
                    var value = readTimestampWithTimeZone(result, column);
                    if (value == null) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, epochMicros(value.toInstant()));
                    }
                };
            }
            case DECIMAL -> {
                var target = (VarCharVector) vector;
                yield (result, row) -> {
                    var value = result.getBigDecimal(column);
                    if (value == null) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value.toPlainString().getBytes(StandardCharsets.UTF_8));
                    }
                };
            }
            case VARCHAR, JSONB -> {
                var target = (VarCharVector) vector;
                yield (result, row) -> {
                    var value = result.getString(column);
                    if (value == null) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value.getBytes(StandardCharsets.UTF_8));
                    }
                };
            }
            case BYTEA -> {
                var target = (VarBinaryVector) vector;
                yield (result, row) -> {
                    var value = result.getBytes(column);
                    if (value == null) {
                        target.setNull(row);
                    } else {
                        target.setSafe(row, value);
                    }
                };
            }
            default ->
                    throw new SQLException(
                            "Unsupported RisingWave type for Oracle snapshot: " + typeName);
        };
    }

    private static OffsetDateTime readTimestampWithTimeZone(ResultSet result, int column)
            throws SQLException {
        try {
            return result.getObject(column, OffsetDateTime.class);
        } catch (SQLException unsupportedConversion) {
            var value = result.getTimestamp(column);
            return value == null ? null : value.toLocalDateTime().atOffset(ZoneOffset.UTC);
        }
    }

    private static long epochMicros(LocalDateTime value) {
        return epochMicros(value.toInstant(ZoneOffset.UTC));
    }

    private static long epochMicros(Instant value) {
        return Math.addExact(
                Math.multiplyExact(value.getEpochSecond(), 1_000_000L), value.getNano() / 1_000);
    }

    @FunctionalInterface
    private interface ColumnWriter {
        void write(ResultSet result, int row) throws SQLException;
    }
}
