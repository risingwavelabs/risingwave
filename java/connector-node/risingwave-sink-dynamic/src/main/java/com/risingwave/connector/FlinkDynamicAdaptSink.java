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

package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import com.risingwave.connector.sinkwriter.SinkFunctionImpl;
import com.risingwave.connector.sinkwriter.SinkWriterAdapt;
import com.risingwave.connector.sinkwriter.SinkWriterImpl;
import com.risingwave.connector.sinkwriter.SinkWriterV2Impl;
import java.util.Iterator;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

public class FlinkDynamicAdaptSink<CommT> extends SinkWriterBase {
    SinkWriterAdapt<CommT> sinkWriterAdapt;

    public FlinkDynamicAdaptSink(TableSchema tableSchema, Sink<RowData, CommT, ?, ?> sink) {
        super(tableSchema);
        this.sinkWriterAdapt = new SinkWriterImpl<CommT>(sink);
    }

    public FlinkDynamicAdaptSink(TableSchema tableSchema, SinkFunction<RowData> sinkFunction) {
        super(tableSchema);
        this.sinkWriterAdapt = new SinkFunctionImpl(sinkFunction);
    }

    public FlinkDynamicAdaptSink(
            TableSchema tableSchema, org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
        super(tableSchema);
        this.sinkWriterAdapt = new SinkWriterV2Impl(sink);
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            sinkWriterAdapt.writer(new RowDataImpl(row, getTableSchema().getNumColumns()));
        }
    }

    @Override
    public void sync() {
        sinkWriterAdapt.commit();
    }

    @Override
    public void drop() {
        sinkWriterAdapt.drop();
    }
}
