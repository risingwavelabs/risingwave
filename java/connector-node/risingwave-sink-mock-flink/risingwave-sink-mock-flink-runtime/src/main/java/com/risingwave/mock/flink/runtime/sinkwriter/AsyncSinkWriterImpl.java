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
import com.risingwave.connector.api.sink.SinkWriterBase;
import com.risingwave.mock.flink.runtime.RowDataImpl;
import com.risingwave.mock.flink.runtime.context.SinkWriterContextV2;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

/**
 * AsyncSinkWriter is the special case of stateful sink, which is also the only stateful support we
 * currently provide. It involves writing data to the sink by closing the async channel and then
 * re-creating a writer for subsequent writing. This is different from the usage in Flink.
 */
public class AsyncSinkWriterImpl extends SinkWriterBase {
    org.apache.flink.api.connector.sink2.Sink<RowData> sink;

    SinkWriter<RowData> writer;

    public AsyncSinkWriterImpl(
            TableSchema tableSchema, org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
        super(tableSchema);
        try {
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContextV2());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            writeRow(new RowDataImpl(row, getTableSchema().getNumColumns()));
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
    public void sync() {
        try {
            if (writer == null) {
                return;
            }
            this.writer.flush(true);
            closeWriter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
