package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
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
        SinkRow outcome = deserializer.deserialize(jsonPayload).next();
        assertEquals(outcome.get(0), 1);
        assertEquals(outcome.get(1), "John");
    }
}
