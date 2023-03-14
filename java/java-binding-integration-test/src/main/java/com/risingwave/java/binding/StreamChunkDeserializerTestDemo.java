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

package com.risingwave.java.binding;

import com.risingwave.connector.StreamChunkDeserializer;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import java.io.IOException;
import java.util.Iterator;

public class StreamChunkDeserializerTestDemo {

    public static void main(String[] args) throws IOException {
        byte[] payload = System.in.readAllBytes();
        TableSchema tableSchema = Utils.getMockTableSchema();
        StreamChunkDeserializer deseriablizer = new StreamChunkDeserializer(tableSchema);
        Iterator<SinkRow> iter = deseriablizer.deserialize(payload);
        int count = 0;
        while (true) {
            SinkRow row = iter.next();
            if (row == null) {
                break;
            }
            count += 1;
            Utils.validateSinkRow(row);
        }
        int expectedCount = 30000;
        if (count != expectedCount) {
            throw new RuntimeException(
                    String.format("row count is %s, should be %s", count, expectedCount));
        }
    }
}
