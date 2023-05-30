package com.risingwave.functions;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.functions.example.*;

/**
 * Unit test for simple App.
 */
public class TestUdfServer {
    @Test
    public void gcd() throws Exception {
        var allocator = new RootAllocator();
        try (var server = new UdfServer("localhost", 0)) {
            server.addFunction("gcd", new Gcd());
            server.start();

            try (var client = new UdfClient("localhost", server.getPort())) {
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
        }
    }
}
