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

package com.risingwave.connector.api.sink;

import com.risingwave.proto.Data;

public class ArraySinkrow implements SinkRow {
    public final Object[] values;
    public final Data.Op op;

    public ArraySinkrow(Data.Op op, Object... value) {
        this.op = op;
        this.values = value;
    }

    @Override
    public Object get(int index) {
        return values[index];
    }

    @Override
    public Data.Op getOp() {
        return op;
    }

    @Override
    public int size() {
        return values.length;
    }
}
