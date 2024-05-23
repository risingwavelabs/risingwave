// Copyright 2024 RisingWave Labs
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

package com.risingwave.connector.deserializer;

import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import java.util.List;

public class ArrowRow implements SinkRow {

    private final List<Object> columns;
    private final Data.Op opValue;

    public ArrowRow(List<Object> columns, short op) {
        Data.Op opValue = Data.Op.forNumber(op);
        this.columns = columns;
        this.opValue = opValue;
    }

    @Override
    public Object get(int index) {
        return columns.get(index);
    }

    @Override
    public Data.Op getOp() {
        return opValue;
    }

    @Override
    public int size() {
        return columns.size();
    }
}
