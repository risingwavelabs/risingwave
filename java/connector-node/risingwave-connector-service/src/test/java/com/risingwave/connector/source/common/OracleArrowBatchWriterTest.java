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

import static org.junit.Assert.assertEquals;

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.PlanCommon;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TimeZone;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OracleArrowBatchWriterTest {
    private TimeZone defaultTimeZone;

    @Before
    public void saveDefaultTimeZone() {
        defaultTimeZone = TimeZone.getDefault();
    }

    @After
    public void restoreDefaultTimeZone() {
        TimeZone.setDefault(defaultTimeZone);
    }

    @Test
    public void timestampInDaylightSavingGapIsNotShifted() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        var gap = LocalDateTime.of(2024, 3, 10, 2, 30);
        var schema =
                tableSchema(
                        Map.of(
                                "TIMESTAMP_VALUE", Data.DataType.TypeName.TIMESTAMP,
                                "TIMESTAMPLTZ_VALUE", Data.DataType.TypeName.TIMESTAMPTZ),
                        "TIMESTAMP_VALUE",
                        "TIMESTAMPLTZ_VALUE");

        try (var allocator = new RootAllocator();
                var writer = new OracleArrowBatchWriter(allocator, schema)) {
            writer.append(resultSet(Map.of(1, gap, 2, gap)));
            try (var reader =
                    new ArrowStreamReader(
                            new ByteArrayInputStream(writer.finishBatch()), allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
                var expected = gap.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
                assertEquals(expected, ((TimeStampMicroVector) root.getVector(0)).get(0));
                assertEquals(expected, ((TimeStampMicroTZVector) root.getVector(1)).get(0));
            }
        }
    }

    @Test
    public void dateSkippedByTimeZoneIsNotShifted() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Apia"));
        var skipped = LocalDate.of(2011, 12, 30);
        var schema = tableSchema(Map.of("DATE_VALUE", Data.DataType.TypeName.DATE), "DATE_VALUE");

        try (var allocator = new RootAllocator();
                var writer = new OracleArrowBatchWriter(allocator, schema)) {
            writer.append(resultSet(Map.of(1, skipped)));
            try (var reader =
                    new ArrowStreamReader(
                            new ByteArrayInputStream(writer.finishBatch()), allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                reader.loadNextBatch();
                assertEquals(
                        Math.toIntExact(skipped.toEpochDay()),
                        ((DateDayVector) root.getVector(0)).get(0));
            }
        }
    }

    private static ConnectorServiceProto.TableSchema tableSchema(
            Map<String, Data.DataType.TypeName> types, String... columnNames) {
        var builder = ConnectorServiceProto.TableSchema.newBuilder();
        for (var name : columnNames) {
            builder.addColumns(
                    PlanCommon.ColumnDesc.newBuilder()
                            .setName(name)
                            .setColumnType(Data.DataType.newBuilder().setTypeName(types.get(name)))
                            .build());
        }
        return builder.build();
    }

    /**
     * A result row whose legacy getters follow the JDBC rule for zone-less values: they interpret
     * the wall-clock value in the JVM default time zone. TIMESTAMP WITH LOCAL TIME ZONE columns do
     * not convert to {@code OffsetDateTime}, as with the Oracle driver.
     */
    private static ResultSet resultSet(Map<Integer, Object> values) {
        return (ResultSet)
                Proxy.newProxyInstance(
                        ResultSet.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "getObject" -> {
                                        if (args[1] == OffsetDateTime.class) {
                                            throw new SQLException("unsupported conversion");
                                        }
                                        yield ((Class<?>) args[1]).cast(values.get(args[0]));
                                    }
                                    case "getTimestamp" ->
                                            java.sql.Timestamp.valueOf(
                                                    (LocalDateTime) values.get(args[0]));
                                    case "getDate" ->
                                            java.sql.Date.valueOf((LocalDate) values.get(args[0]));
                                    case "wasNull" -> false;
                                    default ->
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                });
    }
}
