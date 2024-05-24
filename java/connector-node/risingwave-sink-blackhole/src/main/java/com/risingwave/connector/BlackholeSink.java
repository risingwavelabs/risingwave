/*
 * Copyright 2024 RisingWave Labs
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
import java.util.Iterator;

public class BlackholeSink extends SinkWriterBase {
    BlackholeConfig config;

    public BlackholeSink(BlackholeConfig config, TableSchema tableSchema) {
        super(tableSchema);
        this.config = config;
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        TableSchema schema = getTableSchema();
        int columnDescs = schema.getNumColumns();
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            for (int i = 0; i < columnDescs; i++) {
                var a = row.get(i);
            }
        }
    }

    @Override
    public void sync() {}

    @Override
    public void drop() {}
}
