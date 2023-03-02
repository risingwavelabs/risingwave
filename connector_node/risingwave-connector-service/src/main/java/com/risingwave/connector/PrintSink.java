package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;

public class PrintSink extends SinkBase {
    private PrintStream out = System.out;

    public PrintSink(Map<String, String> properties, TableSchema tableSchema) {
        super(tableSchema);
        out.println("PrintSink: initialized with config: " + properties);
    }

    public PrintSink(Map<String, String> properties, TableSchema tableSchema, PrintStream out) {
        super(tableSchema);
        this.out = out;
        out.println("PrintSink: initialized with config: " + properties);
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            out.println(
                    "PrintSink: "
                            + row.getOp().name()
                            + " values "
                            + Arrays.toString(
                                    IntStream.range(0, row.size()).mapToObj(row::get).toArray()));
        }
    }

    @Override
    public void sync() {
        out.println("PrintSink: sync sink");
    }

    @Override
    public void drop() {
        out.println("PrintSink: drop sink");
    }
}
