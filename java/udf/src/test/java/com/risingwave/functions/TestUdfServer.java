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
import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
        server.addFunction("to_string", new ToString());
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
            assertEquals(output.contentToTSVString().trim(), "3");
        }
    }

    public static class ToString implements ScalarFunction {
        public String eval(String s) {
            return s;
        }
    }

    @Test
    public void to_string() throws Exception {
        var c0 = new VarCharVector("", allocator);
        c0.allocateNew(1);
        c0.set(0, "string".getBytes());
        c0.setValueCount(1);
        var input = VectorSchemaRoot.of(c0);

        try (var stream = client.call("to_string", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals(output.contentToTSVString().trim(), "string");
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
            assertEquals(output.contentToTSVString(), "row_index\t\n1\t0\n2\t0\n2\t1\n");
        }
    }
}
