package com.risingwave.execution.result;

import com.google.common.collect.Lists;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.data.Buffer;
import com.risingwave.proto.data.Column;
import com.risingwave.proto.data.DataChunk;
import com.risingwave.proto.data.DataType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BatchDataChunkResultTest {

  @Test
  public void testEmptyRemoteBatchPlanResult() {
    ArrayList<TaskData> taskDataList = new ArrayList<>();
    RisingWaveTypeFactory typeFactory = new RisingWaveTypeFactory();
    var relDataTypes = Lists.newArrayList(typeFactory.createSqlType(SqlTypeName.INTEGER));
    var fieldNames = Lists.newArrayList("abc");
    for (int i = 0; i < 4; ++i) {
      taskDataList.add(TaskData.newBuilder().build());
    }
    BatchDataChunkResult ret =
        new BatchDataChunkResult(
            StatementType.SELECT,
            taskDataList,
            typeFactory.createStructType(relDataTypes, fieldNames));
    PgResult.PgIter iter = ret.createIterator();
    Assertions.assertFalse(iter.next());
    Assertions.assertFalse(iter.next());
  }

  @Test
  public void testEmptyQueryResult() {
    RisingWaveTypeFactory typeFactory = new RisingWaveTypeFactory();
    // No column.
    var rowType = typeFactory.createStructType(Lists.newArrayList(), Lists.newArrayList());
    BatchDataChunkResult ret =
        new BatchDataChunkResult(
            StatementType.INSERT, Lists.newArrayList() /*empty result*/, rowType);
    PgResult.PgIter iter = ret.createIterator();
    Assertions.assertFalse(iter.next());
    Assertions.assertFalse(iter.next());
  }

  @Test
  public void testSimpleRemoteBatchPlanResult() {
    ArrayList<TaskData> taskDataList = new ArrayList<TaskData>();
    RisingWaveTypeFactory typeFactory = new RisingWaveTypeFactory();
    var relDataTypes = Lists.newArrayList(typeFactory.createSqlType(SqlTypeName.INTEGER));
    var fieldNames = Lists.newArrayList("abc");
    for (int i = 0; i < 4; ++i) {
      TaskData data =
          TaskData.newBuilder()
              .setRecordBatch(
                  DataChunk.newBuilder()
                      .addColumns(
                          Any.pack(
                              Column.newBuilder()
                                  .setColumnType(
                                      DataType.newBuilder().setTypeName(DataType.TypeName.INT32))
                                  .addValues(
                                      Buffer.newBuilder()
                                          .setBody(
                                              ByteString.copyFrom(
                                                  ByteBuffer.allocate(4).putInt(i).array())))
                                  .build()))
                      .addColumns(
                          Any.pack(
                              Column.newBuilder()
                                  .setColumnType(
                                      DataType.newBuilder().setTypeName(DataType.TypeName.BOOLEAN))
                                  .addValues(
                                      Buffer.newBuilder()
                                          .setBody(
                                              ByteString.copyFrom(
                                                  ByteBuffer.allocate(2)
                                                      .put(Byte.parseByte(i % 2 + "", 2))
                                                      .array())))
                                  .build()))
                      .setCardinality(1))
              .build();
      taskDataList.add(data);
    }
    BatchDataChunkResult ret =
        new BatchDataChunkResult(
            StatementType.SELECT,
            taskDataList,
            typeFactory.createStructType(relDataTypes, fieldNames));
    PgResult.PgIter iter = ret.createIterator();
    Assertions.assertTrue(iter.next());
    List<PgValue> row = iter.getRow();
    Assertions.assertEquals("0", row.get(0).encodeInText());
    Assertions.assertEquals("false", row.get(1).encodeInText());
    Assertions.assertTrue(iter.next());
    row = iter.getRow();
    Assertions.assertEquals("1", row.get(0).encodeInText());
    Assertions.assertEquals("true", row.get(1).encodeInText());
    Assertions.assertTrue(iter.next());
    row = iter.getRow();
    Assertions.assertEquals("2", row.get(0).encodeInText());
    Assertions.assertEquals("false", row.get(1).encodeInText());
    Assertions.assertTrue(iter.next());
    row = iter.getRow();
    Assertions.assertEquals("3", row.get(0).encodeInText());
    Assertions.assertEquals("true", row.get(1).encodeInText());
    Assertions.assertFalse(iter.next());
  }
}
