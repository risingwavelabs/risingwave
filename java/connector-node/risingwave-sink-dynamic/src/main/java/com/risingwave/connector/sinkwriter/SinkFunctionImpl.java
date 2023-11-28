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

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

/*
 * The SinkFunction-type sink will invoke this class to write RW data to the downstream.
 */
public class SinkFunctionImpl implements SinkWriterAdapt {
    SinkFunction<RowData> sinkFunction;

    public SinkFunctionImpl(SinkFunction<RowData> sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    @Override
    public void commit() {
        try {
            sinkFunction.finish();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writer(RowData rowData) {
        try {
            sinkFunction.invoke(
                    rowData,
                    new SinkFunction.Context() {
                        @Override
                        public long currentProcessingTime() {
                            return 0;
                        }

                        @Override
                        public long currentWatermark() {
                            return 0;
                        }

                        @Override
                        public Long timestamp() {
                            return null;
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort() {
        return;
    }

    @Override
    public void drop() {
        return;
    }
}
