package com.risingwave.java.binding;

import com.google.common.collect.Lists;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import java.io.IOException;

public class StreamChunkDeserializerDemo {
    public static void main(String[] args) throws IOException {
        //        byte[] binaryData = System.in.readAllBytes();
        //        TableSchema tableSchema = getMockTableSchema();
        //        StreamChunkDeserializer deseriablizer = new StreamChunkDeserializer(tableSchema);
        //        StreamChunkPayload payload =
        //                StreamChunkPayload.newBuilder()
        //                        .setBinaryData(ByteString.copyFrom(binaryData))
        //                        .build();
        //        Iterator<SinkRow> iter = deseriablizer.deserialize(payload);
        //        int count = 0;
        //        while (true) {
        //            if (!iter.hasNext()) {
        //                break;
        //            }
        //            SinkRow row = iter.next();
        //            count += 1;
        //            validateSinkRow(row);
        //        }
        //        int expectedCount = 30000;
        //        if (count != expectedCount) {
        //            throw new RuntimeException(
        //                    String.format("row count is %s, should be %s", count, expectedCount));
        //        }
    }

    public static TableSchema getMockTableSchema() {
        return new TableSchema(
                Lists.newArrayList("v1", "v2", "v3", "v4", "v5", "v6", "v7", "may_null"),
                Lists.newArrayList(
                        Data.DataType.TypeName.INT16,
                        Data.DataType.TypeName.INT32,
                        Data.DataType.TypeName.INT64,
                        Data.DataType.TypeName.FLOAT,
                        Data.DataType.TypeName.DOUBLE,
                        Data.DataType.TypeName.BOOLEAN,
                        Data.DataType.TypeName.VARCHAR,
                        Data.DataType.TypeName.INT64),
                Lists.newArrayList("v1"));
    }

    public static void validateSinkRow(SinkRow row) {
        // The validation of row data are according to the data generation rule
        // defined in ${REPO_ROOT}/src/java_binding/gen-demo-insert-data.py
        short rowIndex = (short) row.get(0);
        if (row.get(1) instanceof Short && !((Short) row.get(1)).equals(rowIndex)) {
            throw new RuntimeException(
                    String.format("invalid int value: %s %s", row.get(1), rowIndex));
        }
        if (row.get(2) instanceof Long && !((Long) row.get(2)).equals((long) rowIndex)) {
            throw new RuntimeException(
                    String.format("invalid long value: %s %s", row.get(2), rowIndex));
        }
        if (row.get(3) instanceof Float && !((Float) row.get(3)).equals((float) rowIndex)) {
            throw new RuntimeException(
                    String.format("invalid float value: %s %s", row.get(3), rowIndex));
        }
        if (row.get(4) instanceof Double && !((Double) row.get(4)).equals((double) rowIndex)) {
            throw new RuntimeException(
                    String.format("invalid double value: %s %s", row.get(4), rowIndex));
        }
        if (row.get(5) instanceof Boolean && !((Boolean) row.get(5)).equals(rowIndex % 3 == 0)) {
            throw new RuntimeException(
                    String.format("invalid bool value: %s %s", row.get(5), (rowIndex % 3 == 0)));
        }
        if (row.get(6) instanceof String
                && !row.get(6).equals(((Short) rowIndex).toString().repeat((rowIndex % 10) + 1))) {
            throw new RuntimeException(
                    String.format(
                            "invalid string value: %s %s",
                            row.get(6), ((Short) rowIndex).toString().repeat((rowIndex % 10) + 1)));
        }
        if ((row.get(7) == null) != (rowIndex % 5 == 0)) {
            throw new RuntimeException(
                    String.format("invalid isNull value: %s %s", row.get(7), (rowIndex % 5 == 0)));
        }
    }
}
