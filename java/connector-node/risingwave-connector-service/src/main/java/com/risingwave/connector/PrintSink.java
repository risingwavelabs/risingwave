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
            try (SinkRow row = rows.next()) {
                out.println(
                        "PrintSink: "
                                + row.getOp().name()
                                + " values "
                                + Arrays.toString(
                                        IntStream.range(0, row.size())
                                                .mapToObj(row::get)
                                                .toArray()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
