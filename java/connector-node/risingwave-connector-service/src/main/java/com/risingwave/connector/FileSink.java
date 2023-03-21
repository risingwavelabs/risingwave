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

import static io.grpc.Status.*;

import com.google.gson.Gson;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.IntStream;

public class FileSink extends SinkBase {
    private final FileWriter sinkWriter;

    private String sinkPath;

    private boolean closed = false;

    public FileSink(String sinkPath, TableSchema tableSchema) {
        super(tableSchema);
        this.sinkPath = sinkPath;
        try {
            new File(sinkPath).mkdirs();
            Path path = Paths.get(sinkPath, UUID.randomUUID() + ".dat");
            if (path.toFile().createNewFile()) {
                sinkWriter = new FileWriter(path.toFile());
            } else {
                throw INTERNAL.withDescription("failed to create file: " + path)
                        .asRuntimeException();
            }
            this.sinkPath = path.toString();
        } catch (IOException e) {
            throw INTERNAL.withCause(e).asRuntimeException();
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                switch (row.getOp()) {
                    case INSERT:
                        String buf =
                                new Gson()
                                        .toJson(
                                                IntStream.range(0, row.size())
                                                        .mapToObj(row::get)
                                                        .toArray());
                        try {
                            sinkWriter.write(buf + System.lineSeparator());
                        } catch (IOException e) {
                            throw INTERNAL.withCause(e).asRuntimeException();
                        }
                        break;
                    default:
                        throw UNIMPLEMENTED
                                .withDescription("unsupported operation: " + row.getOp())
                                .asRuntimeException();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sync() {
        try {
            sinkWriter.flush();
        } catch (IOException e) {
            throw INTERNAL.withCause(e).asRuntimeException();
        }
    }

    @Override
    public void drop() {
        try {
            sinkWriter.close();
            closed = true;
        } catch (IOException e) {
            throw INTERNAL.withCause(e).asRuntimeException();
        }
    }

    public String getSinkPath() {
        return sinkPath;
    }

    public boolean isClosed() {
        return closed;
    }
}
