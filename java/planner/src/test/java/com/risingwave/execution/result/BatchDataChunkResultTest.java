package com.risingwave.execution.result;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.proto.computenode.GetDataResponse;
import com.risingwave.proto.data.Array;
import com.risingwave.proto.data.ArrayType;
import com.risingwave.proto.data.Buffer;
import com.risingwave.proto.data.Column;
import com.risingwave.proto.data.DataChunk;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BatchDataChunkResultTest {

  @Test
  public void testEmptyRemoteBatchPlanResult() {
    RisingWaveTypeFactory typeFactory = RisingWaveTypeFactory.INSTANCE;
    var relDataTypes = Lists.newArrayList(typeFactory.createSqlType(SqlTypeName.INTEGER));
    var fieldNames = Lists.newArrayList("abc");
    BatchDataChunkResult ret =
        new BatchDataChunkResult(
            StatementType.SELECT,
            ImmutableList.of(),
            typeFactory.createStructType(relDataTypes, fieldNames));
    PgResult.PgIter iter = ret.createIterator();
    Assertions.assertFalse(iter.next());
    Assertions.assertFalse(iter.next());
  }

  @Test
  public void testEmptyQueryResult() {
    RisingWaveTypeFactory typeFactory = RisingWaveTypeFactory.INSTANCE;
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
    ArrayList<GetDataResponse> taskDataList = new ArrayList<GetDataResponse>();
    RisingWaveTypeFactory typeFactory = RisingWaveTypeFactory.INSTANCE;
    var relDataTypes =
        Lists.newArrayList(
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            typeFactory.createSqlType(SqlTypeName.BOOLEAN));
    var fieldNames = Lists.newArrayList("col1", "col2");
    for (int i = 0; i < 4; ++i) {
      GetDataResponse data =
          GetDataResponse.newBuilder()
              .setRecordBatch(
                  DataChunk.newBuilder()
                      .addColumns(
                          Column.newBuilder()
                              .setArray(
                                  Array.newBuilder()
                                      .addValues(
                                          Buffer.newBuilder()
                                              .setBody(
                                                  ByteString.copyFrom(
                                                      ByteBuffer.allocate(4).putInt(i).array())))
                                      .setArrayType(ArrayType.INT32)
                                      .build())
                              .build())
                      .addColumns(
                          Column.newBuilder()
                              .setArray(
                                  Array.newBuilder()
                                      .addValues(
                                          Buffer.newBuilder()
                                              .setBody(
                                                  ByteString.copyFrom(
                                                      ByteBuffer.allocate(2)
                                                          .put(Byte.parseByte(i % 2 + "", 2))
                                                          .array())))
                                      .setArrayType(ArrayType.BOOL)
                                      .build())
                              .build())
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
    Assertions.assertEquals("f", row.get(1).encodeInText());
    Assertions.assertTrue(iter.next());
    row = iter.getRow();
    Assertions.assertEquals("1", row.get(0).encodeInText());
    Assertions.assertEquals("t", row.get(1).encodeInText());
    Assertions.assertTrue(iter.next());
    row = iter.getRow();
    Assertions.assertEquals("2", row.get(0).encodeInText());
    Assertions.assertEquals("f", row.get(1).encodeInText());
    Assertions.assertTrue(iter.next());
    row = iter.getRow();
    Assertions.assertEquals("3", row.get(0).encodeInText());
    Assertions.assertEquals("t", row.get(1).encodeInText());
    Assertions.assertFalse(iter.next());
  }
}
