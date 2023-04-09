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

package com.risingwave.connector;

import static com.risingwave.proto.Data.*;
import static org.junit.Assert.*;

import com.google.common.collect.Iterators;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import org.junit.Test;

public class FileSinkTest {
    @Test
    public void testSync() throws IOException {
        String path = "/tmp/rw-connector-node";
        if (!Paths.get(path).toFile().isDirectory()) {
            Files.createDirectories(Paths.get(path));
        }

        FileSinkConfig config = new FileSinkConfig(path);
        FileSink sink = new FileSink(config, TableSchema.getMockTableSchema());
        String filePath = sink.getSinkPath();

        Path file = Paths.get(filePath);
        try {
            sink.write(Iterators.forArray(new ArraySinkRow(Op.INSERT, 1, "Alice")));
            sink.sync();
            String[] expectedA = {"[1,\"Alice\"]"};
            String[] actualA = Files.lines(file).toArray(String[]::new);
            assertEquals(expectedA.length, actualA.length);
            IntStream.range(0, expectedA.length)
                    .forEach(i -> assertEquals(expectedA[i], actualA[i]));

            sink.write(Iterators.forArray(new ArraySinkRow(Op.INSERT, 2, "Bob")));
            String[] expectedB = new String[] {"[1,\"Alice\"]"};
            String[] actualB = Files.lines(file).toArray(String[]::new);
            assertEquals(expectedB.length, actualB.length);
            IntStream.range(0, expectedA.length)
                    .forEach(i -> assertEquals(expectedB[i], actualB[i]));

            sink.sync();
            String[] expectedC = {"[1,\"Alice\"]", "[2,\"Bob\"]"};
            String[] actualC = Files.lines(file).toArray(String[]::new);
            assertEquals(expectedC.length, actualC.length);
            IntStream.range(0, expectedC.length)
                    .forEach(i -> assertEquals(expectedC[i], actualC[i]));

        } catch (IOException e) {
            fail("IO exception: " + e);
        } finally {
            sink.drop();
            Files.delete(file);
        }
    }

    @Test
    public void testWrite() throws IOException {
        String path = "/tmp/rw-connector-node";
        if (!Paths.get(path).toFile().isDirectory()) {
            Files.createDirectories(Paths.get(path));
        }
        FileSinkConfig config = new FileSinkConfig(path);
        FileSink sink = new FileSink(config, TableSchema.getMockTableSchema());

        String filePath = sink.getSinkPath();
        try {
            // test write consistency
            String[] expected = {"[1,\"Alice\"]", "[2,\"Bob\"]"};
            sink.write(
                    Iterators.forArray(
                            new ArraySinkRow(Op.INSERT, 1, "Alice"),
                            new ArraySinkRow(Op.INSERT, 2, "Bob")));

            sink.sync();
            String[] actual = Files.lines(Paths.get(filePath)).toArray(String[]::new);
            IntStream.range(0, expected.length).forEach(i -> assertEquals(expected[i], actual[i]));
        } catch (IOException e) {
            fail("IO exception: " + e);
        } finally {
            sink.drop();
            Files.delete(Paths.get(filePath));
        }
    }

    @Test
    public void testDrop() throws IOException {
        String path = "/tmp/rw-connector-node";
        if (!Paths.get(path).toFile().isDirectory()) {
            Files.createDirectories(Paths.get(path));
        }
        FileSinkConfig config = new FileSinkConfig(path);
        FileSink sink = new FileSink(config, TableSchema.getMockTableSchema());

        sink.drop();

        assertTrue(sink.isClosed());
        assertTrue(Files.exists(Paths.get(sink.getSinkPath())));
    }
}
