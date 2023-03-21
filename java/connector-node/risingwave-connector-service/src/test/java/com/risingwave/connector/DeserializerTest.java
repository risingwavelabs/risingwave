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
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.JsonPayload;
import com.risingwave.proto.Data;
import junit.framework.TestCase;

public class DeserializerTest extends TestCase {
    public void testJsonDeserializer() {
        JsonDeserializer deserializer = new JsonDeserializer(TableSchema.getMockTableSchema());
        JsonPayload jsonPayload =
                JsonPayload.newBuilder()
                        .addRowOps(
                                JsonPayload.RowOp.newBuilder()
                                        .setOpType(Data.Op.INSERT)
                                        .setLine("{\"id\": 1, \"name\": \"John\"}")
                                        .build())
                        .build();
        ConnectorServiceProto.SinkStreamRequest.WriteBatch writeBatch =
                ConnectorServiceProto.SinkStreamRequest.WriteBatch.newBuilder()
                        .setJsonPayload(jsonPayload)
                        .build();
        SinkRow outcome = deserializer.deserialize(writeBatch).next();
        assertEquals(outcome.get(0), 1);
        assertEquals(outcome.get(1), "John");
    }
}
