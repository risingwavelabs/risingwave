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

import com.risingwave.connector.context.SinkWriterContextV2;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

public class SinkWriterV2Impl implements SinkWriterAdapt {

    org.apache.flink.api.connector.sink2.Sink<RowData> sink;
    SinkWriter<RowData> writer;

    public SinkWriterV2Impl(org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
        try {
            this.sink = sink;
            this.writer = sink.createWriter(new SinkWriterContextV2());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit() {
        if (writer == null) {
            return;
        }
        try {
            this.writer.flush(true);
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
