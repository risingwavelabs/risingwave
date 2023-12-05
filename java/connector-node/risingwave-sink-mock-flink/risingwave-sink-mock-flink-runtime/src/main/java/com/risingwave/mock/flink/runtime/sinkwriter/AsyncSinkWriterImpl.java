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
import com.risingwave.mock.flink.runtime.context.SinkWriterContextV2;
import com.risingwave.proto.ConnectorServiceProto;
import java.io.IOException;
import java.util.Optional;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

/**
 * AsyncSinkWriter is the special case of stateful sink, which is also the only stateful support we
 * currently provide. It involves writing data to the sink by closing the async writer and then
 * re-creating a writer for subsequent writing. This is different from the usage in Flink.
 */
public class AsyncSinkWriterImpl implements com.risingwave.connector.api.sink.SinkWriter {
    final org.apache.flink.api.connector.sink2.Sink<RowData> sink;

    SinkWriter<RowData> writer;
    final TableSchema tableSchema;

    public AsyncSinkWriterImpl(
            TableSchema tableSchema, org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
        this.tableSchema = tableSchema;
        try {
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContextV2());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeRow(RowData rowData) {
        try {
            if (this.writer == null) {
                this.writer = sink.createWriter(new SinkWriterContextV2());
            }
            this.writer.write(
                    rowData,
                    new SinkWriter.Context() {
                        @Override
                        public long currentWatermark() {
                            return 0;
                        }

                        @Override
                        public Long timestamp() {
                            return null;
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
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
                if (writer != null) {
                    this.writer.flush(true);
                    // The reason we need to close writer here is that:
                    // Flink's async doesn't provide a commit method in the same sense as RW, so
                    // here we are
                    // using close writer for commit-like behavior, but closing writer will cause
                    // the old
                    // writer to no longer be able to write, so we need to close it here and
                    // recreate it the
                    // next time we need to write the data
                    closeWriter();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void drop() {
        closeWriter();
    }

    private void closeWriter() {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
            this.writer = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
