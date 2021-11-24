package com.risingwave.rpc;

import com.risingwave.proto.metadatanode.CreateRequest;
import com.risingwave.proto.metadatanode.Database;
import com.risingwave.proto.metadatanode.DropRequest;
import com.risingwave.proto.metadatanode.HeartbeatRequest;
import com.risingwave.proto.metadatanode.Schema;
import com.risingwave.proto.metadatanode.Table;
import com.risingwave.proto.plan.DatabaseRefId;
import com.risingwave.proto.plan.SchemaRefId;
import com.risingwave.proto.plan.TableRefId;

/** Protobuf static helpers. */
public class MetaMessages {
  public static HeartbeatRequest buildHeartbeatRequest() {
    return HeartbeatRequest.newBuilder().setNodeType(HeartbeatRequest.NodeType.FRONTEND).build();
  }

  public static CreateRequest buildCreateDatabaseRequest(Database database) {
    return CreateRequest.newBuilder().setDatabase(database).build();
  }

  public static CreateRequest buildCreateSchemaRequest(Schema schema) {
    return CreateRequest.newBuilder().setSchema(schema).build();
  }

  public static CreateRequest buildCreateTableRequest(Table table) {
    return CreateRequest.newBuilder().setTable(table).build();
  }

  public static DropRequest buildDropDatabaseRequest(DatabaseRefId databaseRefId) {
    return DropRequest.newBuilder().setDatabaseId(databaseRefId).build();
  }

  public static DropRequest buildDropSchemaRequest(SchemaRefId schemaRefId) {
    return DropRequest.newBuilder().setSchemaId(schemaRefId).build();
  }

  public static DropRequest buildDropTableRequest(TableRefId tableRefId) {
    return DropRequest.newBuilder().setTableId(tableRefId).build();
  }
}
