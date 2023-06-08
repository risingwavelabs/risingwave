package com.risingwave.functions;

import java.io.IOException;
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

import com.risingwave.functions.example.*;

/**
 * Unit test for simple App.
 */
public class TestUdfServer {
    private static UdfClient client;
    private static UdfServer server;
    private static BufferAllocator allocator = new RootAllocator();

    @BeforeAll
    public static void setup() throws IOException {
        server = new UdfServer("localhost", 0);
        server.addFunction("gcd", new UdfExample.Gcd());
        server.addFunction("to_string", new UdfExample.ToString());
        server.start();

        client = new UdfClient("localhost", server.getPort());
    }

    @AfterAll
    public static void teardown() throws InterruptedException {
        client.close();
        server.close();
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
}
