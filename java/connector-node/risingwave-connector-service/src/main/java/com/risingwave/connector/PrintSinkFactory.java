// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import java.util.Map;

public class PrintSinkFactory implements SinkFactory {

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        return new PrintSink(tableProperties, tableSchema);
    }

    @Override
    public TableSchema validate(TableSchema tableSchema, Map<String, String> tableProperties) {
        return tableSchema;
    }
}
