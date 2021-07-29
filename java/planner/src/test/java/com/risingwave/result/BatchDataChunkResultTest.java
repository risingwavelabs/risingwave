package com.risingwave.result;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.execution.result.BatchDataChunkResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.data.Buffer;
import com.risingwave.proto.data.ColumnCommon;
import com.risingwave.proto.data.DataChunk;
import com.risingwave.proto.data.FixedWidthNumericColumn;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BatchDataChunkResultTest {

  @Test
  public void testEmptyRemoteBatchPlanResult() {
    ArrayList<TaskData> taskDataList = new ArrayList<TaskData>();
    RisingWaveTypeFactory typeFactory = new RisingWaveTypeFactory();
    ArrayList<RelDataType> relDataTypes = new ArrayList<RelDataType>();
    ArrayList<String> fieldNames = new ArrayList<String>();
    fieldNames.add("abc");
    for (int i = 0; i < 4; ++i) {
      taskDataList.add(TaskData.newBuilder().build());
    }
    relDataTypes.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
    BatchDataChunkResult ret =
        new BatchDataChunkResult(
            StatementType.SELECT,
            false,
            taskDataList,
            typeFactory.createStructType(relDataTypes, fieldNames));
    PgResult.PgIter iter = ret.createIterator();
    Assertions.assertTrue(iter.next() == false);
    Assertions.assertTrue(iter.next() == false);
  }

  @Test
  public void testSimpleRemoteBatchPlanResult() {
    ArrayList<TaskData> taskDataList = new ArrayList<TaskData>();
    RisingWaveTypeFactory typeFactory = new RisingWaveTypeFactory();
    ArrayList<RelDataType> relDataTypes = new ArrayList<RelDataType>();
    ArrayList<String> fieldNames = new ArrayList<String>();
    fieldNames.add("abc");
    for (int i = 0; i < 4; ++i) {
      TaskData data =
          TaskData.newBuilder()
              .setRecordBatch(
                  DataChunk.newBuilder()
                      .addColumns(
                          Any.pack(
                              FixedWidthNumericColumn.newBuilder()
                                  .setCommonParts(
                                      ColumnCommon.newBuilder()
                                          .setColumnType(ColumnCommon.ColumnType.INT32))
                                  .setValues(
                                      Buffer.newBuilder()
                                          .setBody(
                                              ByteString.copyFrom(
                                                  ByteBuffer.allocate(4).putInt(i).array())))
                                  .build()))
                      .addColumns(
                          Any.pack(
                              FixedWidthNumericColumn.newBuilder()
                                  .setCommonParts(
                                      ColumnCommon.newBuilder()
                                          .setColumnType(ColumnCommon.ColumnType.BOOLEAN))
                                  .setValues(
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
    relDataTypes.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
    BatchDataChunkResult ret =
        new BatchDataChunkResult(
            StatementType.SELECT,
            false,
            taskDataList,
            typeFactory.createStructType(relDataTypes, fieldNames));
    PgResult.PgIter iter = ret.createIterator();
    Assertions.assertTrue(iter.next() == true);
    List<PgValue> row = iter.getRow();
    Assertions.assertTrue(row.get(0).encodeInText().equals("0"));
    Assertions.assertTrue(row.get(1).encodeInText().equals("false"));
    Assertions.assertTrue(iter.next() == true);
    row = iter.getRow();
    Assertions.assertTrue(row.get(0).encodeInText().equals("1"));
    Assertions.assertTrue(row.get(1).encodeInText().equals("true"));
    Assertions.assertTrue(iter.next() == true);
    row = iter.getRow();
    Assertions.assertTrue(row.get(0).encodeInText().equals("2"));
    Assertions.assertTrue(row.get(1).encodeInText().equals("false"));
    Assertions.assertTrue(iter.next() == true);
    row = iter.getRow();
    Assertions.assertTrue(row.get(0).encodeInText().equals("3"));
    Assertions.assertTrue(row.get(1).encodeInText().equals("true"));
    Assertions.assertTrue(iter.next() == false);
  }
}
