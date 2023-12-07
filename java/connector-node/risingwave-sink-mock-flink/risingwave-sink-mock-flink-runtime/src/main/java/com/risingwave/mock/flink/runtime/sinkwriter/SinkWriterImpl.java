/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.mock.flink.runtime.sinkwriter;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.mock.flink.runtime.RowDataImpl;
import com.risingwave.mock.flink.runtime.context.SinkWriterContext;
import com.risingwave.proto.ConnectorServiceProto;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.table.data.RowData;

/*
 * The Sink-type sink will invoke this class to write RW data to the downstream.
 */
public class SinkWriterImpl<CommT> implements com.risingwave.connector.api.sink.SinkWriter {
    final Sink<RowData, CommT, ?, ?> sink;
    final org.apache.flink.api.connector.sink.SinkWriter<RowData, CommT, ?> writer;

    final Optional<? extends Committer<CommT>> committer;

    final TableSchema tableSchema;

    public SinkWriterImpl(TableSchema tableSchema, Sink<RowData, CommT, ?, ?> sink) {
        try {
            this.tableSchema = tableSchema;
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContext(), Collections.emptyList());
            this.committer = sink.createCommitter();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeRow(RowData rowData) {
        try {
            writer.write(
                    rowData,
                    new org.apache.flink.api.connector.sink.SinkWriter.Context() {
                        @Override
                        public long currentWatermark() {
                            return 0;
                        }

                        @Override
                        public Long timestamp() {
                            return null;
                        }
                    });
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginEpoch(long epoch) {}

    @Override
    public boolean write(Iterable<SinkRow> rows) {
        for (SinkRow row : rows) {
            writeRow(new RowDataImpl(row, tableSchema.getNumColumns()));
        }
        return true;
    }

    @Override
    public Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint) {
        if (isCheckpoint) {
            try {
                List<CommT> objects = writer.prepareCommit(true);
                if (committer.isPresent()) {
                    committer.get().commit(objects);
                } else if (!objects.isEmpty()) {
                    throw new RuntimeException("Flink sink v1 committer is null");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void drop() {
        try {
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
