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
import com.risingwave.connector.context.SinkWriterContextV2;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.table.data.RowData;

/*
 * The SinkV2-type sink will invoke this class to write RW data to the downstream.
 */
public class SinkWriterV2Impl<CommT> extends SinkWriterBase {

    org.apache.flink.api.connector.sink2.Sink<RowData> sink;
    SinkWriter<RowData> writer;

    Committer<CommT> committer;

    boolean isAsyncSink;

    public SinkWriterV2Impl(
            TableSchema tableSchema, org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
        super(tableSchema);
        try {
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContextV2());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        isAsyncSink = writer instanceof AsyncSinkWriter;
        if (!isAsyncSink && (writer instanceof StatefulSink.StatefulSinkWriter)) {
            throw new UnsupportedOperationException("Don't support StatefulSink");
        }

        try {
            if (this.sink instanceof TwoPhaseCommittingSink) {
                this.committer = ((TwoPhaseCommittingSink<?, CommT>) this.sink).createCommitter();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeRow(RowData rowData) {
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
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            writeRow(new RowDataImpl(row, getTableSchema().getNumColumns()));
        }
    }

    @Override
    public void sync() {
        try {
            if (isAsyncSink) {
                if (writer == null) {
                    return;
                }
                this.writer.flush(true);
            } else {
                this.writer.flush(false);
                if (committer != null) {
                    Collection<CommT> objects =
                            ((TwoPhaseCommittingSink.PrecommittingSinkWriter<?, CommT>) this.writer)
                                    .prepareCommit();
                    if (!objects.isEmpty()) {
                        List<Committer.CommitRequest<CommT>> collect =
                                objects.stream()
                                        .map(
                                                (a) -> {
                                                    return (Committer.CommitRequest<CommT>)
                                                            new CommitRequestImpl<>(a);
                                                })
                                        .collect(Collectors.toList());
                        committer.commit(collect);
                    }
                }
                if (this.writer instanceof TwoPhaseCommittingSink.PrecommittingSinkWriter) {
                    ((TwoPhaseCommittingSink.PrecommittingSinkWriter<?, ?>) this.writer)
                            .prepareCommit();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (isAsyncSink) {
                closeWriter();
                this.writer = null;
            }
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
