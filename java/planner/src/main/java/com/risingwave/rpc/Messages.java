package com.risingwave.rpc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.plan.DatabaseRefId;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.SchemaRefId;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.plan.TaskId;
import com.risingwave.proto.plan.TaskSinkId;
import java.util.Random;
import java.util.UUID;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

/** Protobuf static helpers. */
public class Messages {
  private static final JsonFormat.TypeRegistry PROTOBUF_JSON_TYPE_REGISTRY = createTypeRegistry();
  private static final String PROTO_PACKAGE_NAME = "com.risingwave.proto";

  private static JsonFormat.TypeRegistry createTypeRegistry() {
    try {
      Reflections reflections =
          new Reflections(
              new ConfigurationBuilder()
                  .setUrls(ClasspathHelper.forPackage(PROTO_PACKAGE_NAME))
                  .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner())
                  .filterInputsBy(new FilterBuilder().includePackage(PROTO_PACKAGE_NAME)));

      JsonFormat.TypeRegistry.Builder typeRegistry = JsonFormat.TypeRegistry.newBuilder();

      for (Class<?> klass : reflections.getSubTypesOf(GeneratedMessageV3.class)) {
        Descriptors.Descriptor descriptor =
            (Descriptors.Descriptor) klass.getDeclaredMethod("getDescriptor").invoke(null);
        typeRegistry.add(descriptor);
      }

      return typeRegistry.build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create protobuf type registry!", e);
    }
  }

  public static String jsonFormat(MessageOrBuilder message) {
    try {
      return JsonFormat.printer().usingTypeRegistry(PROTOBUF_JSON_TYPE_REGISTRY).print(message);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to serialize pan to json.", e);
    }
  }

  public static CreateTaskRequest buildCreateTaskRequest(PlanFragment planFragment) {
    TaskId taskId =
        TaskId.newBuilder()
            // FIXME: replace random number with a better .
            .setTaskId(new Random().nextInt(1000000000))
            .setStageId(0)
            .setQueryId(UUID.randomUUID().toString())
            .build();
    return CreateTaskRequest.newBuilder().setTaskId(taskId).setPlan(planFragment).build();
  }

  public static TaskSinkId buildTaskSinkId(TaskId taskId) {
    // TODO: Set SinkId.
    return TaskSinkId.newBuilder().setTaskId(taskId).build();
  }

  /**
   * Utility function to create a table ref id from table id.
   *
   * @param tableId table id
   * @return table ref id
   */
  public static TableRefId getTableRefId(TableCatalog.TableId tableId) {
    return TableRefId.newBuilder()
        .setTableId(tableId.getValue())
        .setSchemaRefId(
            SchemaRefId.newBuilder()
                .setSchemaId(tableId.getParent().getValue())
                .setDatabaseRefId(
                    DatabaseRefId.newBuilder()
                        .setDatabaseId(tableId.getParent().getParent().getValue())))
        .build();
  }
}
