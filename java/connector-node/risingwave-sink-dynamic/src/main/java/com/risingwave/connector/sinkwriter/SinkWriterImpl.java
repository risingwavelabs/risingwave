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

import com.risingwave.connector.context.SinkWriterContext;
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
public class SinkWriterImpl<CommT> implements SinkWriterAdapt {
    Sink<RowData, CommT, ?, ?> sink;
    org.apache.flink.api.connector.sink.SinkWriter<RowData, ?, ?> writer;

    public SinkWriterImpl(Sink<RowData, CommT, ?, ?> sink) {
        try {
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContext(), Collections.emptyList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit() {
        try {
            if (writer == null) {
                return;
            }
            Optional<? extends Committer<CommT>> committer = sink.createCommitter();
            if (committer.isEmpty()) {
                return;
            } else {
                List<CommT> objects = (List<CommT>) writer.prepareCommit(true);
                committer.get().commit(objects);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeWriter();
            this.writer = null;
        }
    }

    @Override
    public void writer(RowData rowData) {
        try {
            if (writer == null) {
                this.writer = sink.createWriter(new SinkWriterContext(), Collections.emptyList());
            }
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
            closeWriter();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            closeWriter();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort() {
        return;
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

    @Override
    public void drop() {
        closeWriter();
    }
}
