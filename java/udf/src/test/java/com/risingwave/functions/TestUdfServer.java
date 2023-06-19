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

package com.risingwave.functions;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for UDF server.
 */
public class TestUdfServer {
    private static UdfClient client;
    private static UdfServer server;
    private static BufferAllocator allocator = new RootAllocator();

    @BeforeAll
    public static void setup() throws IOException {
        server = new UdfServer("localhost", 0);
        server.addFunction("gcd", new Gcd());
        server.addFunction("return_all", new ReturnAll());
        server.addFunction("series", new Series());
        server.start();

        client = new UdfClient("localhost", server.getPort());
    }

    @AfterAll
    public static void teardown() throws InterruptedException {
        client.close();
        server.close();
    }

    public static class Gcd implements ScalarFunction {
        public int eval(int a, int b) {
            while (b != 0) {
                int temp = b;
                b = a % b;
                a = temp;
            }
            return a;
        }
    }

    @Test
    public void gcd() throws Exception {
        var c0 = new IntVector("", allocator);
        c0.allocateNew(1);
        c0.set(0, 15);
        c0.setValueCount(1);

        var c1 = new IntVector("", allocator);
        c1.allocateNew(1);
        c1.set(0, 12);
        c1.setValueCount(1);

        var input = VectorSchemaRoot.of(c0, c1);

        try (var stream = client.call("gcd", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals("3", output.contentToTSVString().trim());
        }
    }

    public static class ReturnAll implements ScalarFunction {
        public static class Row {
            public boolean bool;
            public short i16;
            public int i32;
            public long i64;
            public float f32;
            public double f64;
            public LocalDate date;
            public LocalTime time;
            public LocalDateTime timestamp;
            public PeriodDuration interval;
            public String str;
            public byte[] bytes;
            public @DataTypeHint("JSONB") String jsonb;
        }

        public Row eval(boolean bool, short i16, int i32, long i64, float f32, double f64,
                LocalDate date, LocalTime time, LocalDateTime timestamp, PeriodDuration interval,
                String str, byte[] bytes, @DataTypeHint("JSONB") String jsonb) {
            var row = new Row();
            row.bool = bool;
            row.i16 = i16;
            row.i32 = i32;
            row.i64 = i64;
            row.f32 = f32;
            row.f64 = f64;
            row.date = date;
            row.time = time;
            row.timestamp = timestamp;
            row.interval = interval;
            row.str = str;
            row.bytes = bytes;
            row.jsonb = jsonb;
            return row;
        }
    }

    @Test
    public void all_types() throws Exception {
        var c0 = new BitVector("", allocator);
        c0.allocateNew(1);
        c0.set(0, 1);
        c0.setValueCount(1);

        var c1 = new SmallIntVector("", allocator);
        c1.allocateNew(1);
        c1.set(0, 1);
        c1.setValueCount(1);

        var c2 = new IntVector("", allocator);
        c2.allocateNew(1);
        c2.set(0, 1);
        c2.setValueCount(1);

        var c3 = new BigIntVector("", allocator);
        c3.allocateNew(1);
        c3.set(0, 1);
        c3.setValueCount(1);

        var c4 = new Float4Vector("", allocator);
        c4.allocateNew(1);
        c4.set(0, 1);
        c4.setValueCount(1);

        var c5 = new Float8Vector("", allocator);
        c5.allocateNew(1);
        c5.set(0, 1);
        c5.setValueCount(1);

        var c6 = new DateDayVector("", allocator);
        c6.allocateNew(1);
        c6.set(0, (int) LocalDate.of(2023, 1, 1).toEpochDay());
        c6.setValueCount(1);

        var c7 = new TimeMicroVector("", allocator);
        c7.allocateNew(1);
        c7.set(0, LocalTime.of(1, 2, 3).toNanoOfDay() / 1000);
        c7.setValueCount(1);

        var c8 = new TimeStampMicroVector("", allocator);
        c8.allocateNew(1);
        var ts = LocalDateTime.of(2023, 1, 1, 1, 2, 3);
        c8.set(0, ts.toLocalDate().toEpochDay() * 24 * 3600 * 1000000 + ts.toLocalTime().toNanoOfDay() / 1000);
        c8.setValueCount(1);

        var c9 = new IntervalMonthDayNanoVector("", FieldType.nullable(MinorType.INTERVALMONTHDAYNANO.getType()),
                allocator);
        c9.allocateNew(1);
        c9.set(0, 1, 2, 3);
        c9.setValueCount(1);

        var c10 = new VarCharVector("", allocator);
        c10.allocateNew(1);
        c10.set(0, "string".getBytes());
        c10.setValueCount(1);

        var c11 = new VarBinaryVector("", allocator);
        c11.allocateNew(1);
        c11.set(0, "bytes".getBytes());
        c11.setValueCount(1);

        var c12 = new LargeVarCharVector("", allocator);
        c12.allocateNew(1);
        c12.set(0, "{ key: 1 }".getBytes());
        c12.setValueCount(1);

        var input = VectorSchemaRoot.of(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12);

        try (var stream = client.call("return_all", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals(
                    "{\"bool\":true,\"i16\":1,\"i32\":1,\"i64\":1,\"f32\":1.0,\"f64\":1.0,\"date\":19358,\"time\":3723000000,\"timestamp\":[2023,1,1,1,2,3],\"interval\":{\"period\":\"P1M2D\",\"duration\":3E-9},\"str\":\"string\",\"bytes\":\"Ynl0ZXM=\",\"jsonb\":\"{ key: 1 }\"}",
                    output.contentToTSVString().trim());
        }
    }

    public static class Series implements TableFunction {
        public Iterator<Integer> eval(int n) {
            return IntStream.range(0, n).iterator();
        }
    }

    @Test
    public void series() throws Exception {
        var c0 = new IntVector("", allocator);
        c0.allocateNew(3);
        c0.set(0, 0);
        c0.set(1, 1);
        c0.set(2, 2);
        c0.setValueCount(3);

        var input = VectorSchemaRoot.of(c0);

        try (var stream = client.call("series", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals("row_index\t\n1\t0\n2\t0\n2\t1\n", output.contentToTSVString());
        }
    }
}
