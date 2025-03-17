// Copyright 2025 RisingWave Labs
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
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.IntStream;

// `FileSink` only serves for testing purpose. It is NOT the file sink in RisingWave docs.
// TODO: consider rename or remove it to avoid confusion
public class FileSink extends SinkWriterBase {
    private final FileWriter sinkWriter;

    private FileSinkConfig config;

    private boolean closed = false;

    public FileSink(FileSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);

        String sinkPath = config.getSinkPath();
        try {
            new File(sinkPath).mkdirs();
            Path path = Paths.get(sinkPath, UUID.randomUUID() + ".dat");
            if (path.toFile().createNewFile()) {
                sinkWriter = new FileWriter(path.toFile());
            } else {
                throw INTERNAL.withDescription("failed to create file: " + path)
                        .asRuntimeException();
            }
            config.setSinkPath(path.toString());
        } catch (IOException e) {
            throw INTERNAL.withCause(e).asRuntimeException();
        }

        this.config = config;
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
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
        return config.getSinkPath();
    }

    public boolean isClosed() {
        return closed;
    }
}
