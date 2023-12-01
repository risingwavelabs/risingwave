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

package com.risingwave.connector.sinkwriter;

import com.risingwave.connector.RowDataImpl;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import com.risingwave.connector.context.SinkWriterContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.table.data.RowData;

/*
 * The Sink-type sink will invoke this class to write RW data to the downstream.
 */
public class SinkWriterImpl<CommT> extends SinkWriterBase {
    Sink<RowData, CommT, ?, ?> sink;
    org.apache.flink.api.connector.sink.SinkWriter<RowData, CommT, ?> writer;

    Optional<? extends Committer<CommT>> committer;

    public SinkWriterImpl(TableSchema tableSchema, Sink<RowData, CommT, ?, ?> sink) {
        super(tableSchema);
        try {
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContext(), Collections.emptyList());
            this.committer = sink.createCommitter();
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync() {
        try {
            if (committer.isEmpty()) {
                return;
            } else {
                List<CommT> objects = writer.prepareCommit(true);
                committer.get().commit(objects);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
