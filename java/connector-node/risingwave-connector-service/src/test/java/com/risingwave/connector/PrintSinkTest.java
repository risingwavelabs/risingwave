package com.risingwave.connector;

import static com.risingwave.proto.Data.*;

import com.google.common.collect.Iterators;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkrow;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Assert;

public class PrintSinkTest extends TestCase {

    public void testWrite() {
        final boolean[] writeCalled = new boolean[1];

        PrintStream mock =
                new PrintStream(System.out) {
                    private final List<String> expectedOutput =
                            List.of(
                                    "PrintSink: initialized with config: {}",
                                    "PrintSink: INSERT values [1, Alice]",
                                    "PrintSink: UPDATE_DELETE values [1, Alice]",
                                    "PrintSink: UPDATE_INSERT values [2, Bob]",
                                    "PrintSink: DELETE values [2, Bob]");
                    private final Iterator<String> expectedOutputIterator =
                            expectedOutput.iterator();

                    @Override
                    public void print(String x) {
                        writeCalled[0] = true;
                        while (expectedOutputIterator.hasNext()) {
                            String expected = expectedOutputIterator.next();
                            if (expected.equals(x)) {
                                return;
                            } else {
                                fail(
                                        "Unexpected print message: `"
                                                + x
                                                + "`, expected: "
                                                + expected);
                            }
                        }
                        fail("Unexpected print message: `" + x + "`, expected no more messages");
                    }
                };

        PrintSink sink = new PrintSink(new HashMap<>(), TableSchema.getMockTableSchema(), mock);

        sink.write(
                Iterators.forArray(
                        new ArraySinkrow(Op.INSERT, 1, "Alice"),
                        new ArraySinkrow(Op.UPDATE_DELETE, 1, "Alice"),
                        new ArraySinkrow(Op.UPDATE_INSERT, 2, "Bob"),
                        new ArraySinkrow(Op.DELETE, 2, "Bob")));
        if (!writeCalled[0]) {
            fail("write batch did not print messages");
        }
    }

    public void testSync() {
        final boolean[] syncCalled = new boolean[1];
        PrintStream mock =
                new PrintStream(System.out) {
                    @Override
                    public void print(String x) {
                        Assert.assertNotNull("sync sink captured null messages", x);
                        if (x.contains("init") || !x.contains("PrintSink")) {
                            return;
                        }
                        if (x.equals("PrintSink: sync sink")) {
                            syncCalled[0] = true;
                        } else {
                            fail("Unexpected print message: `" + x + "`, expected: sync sink");
                        }
                    }
                };
        PrintSink sink = new PrintSink(new HashMap<>(), TableSchema.getMockTableSchema(), mock);
        sink.sync();
        if (!syncCalled[0]) {
            fail("sync sink did not print messages");
        }
    }

    public void testDrop() {
        final boolean[] dropCalled = {false};
        PrintStream mock =
                new PrintStream(System.out) {
                    @Override
                    public void print(String x) {
                        Assert.assertNotNull("sync sink captured null messages", x);
                        if (x.contains("init") || !x.contains("PrintSink")) {
                            return;
                        }
                        if (!x.equals("PrintSink: drop sink")) {
                            fail("Unexpected print message: `" + x + "`, expected: drop sink");
                        }
                        dropCalled[0] = true;
                    }
                };

        PrintSink sink = new PrintSink(new HashMap<>(), TableSchema.getMockTableSchema(), mock);
        sink.drop();

        if (!dropCalled[0]) {
            fail("drop sink did not print messages");
        }
    }
}
