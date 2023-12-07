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

package com.risingwave.connector.sink.flink;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.mock.flink.common.FlinkDynamicAdapterConfig;
import com.risingwave.mock.flink.common.FlinkMockSinkFactory;
import com.risingwave.mock.flink.runtime.FlinkDynamicAdapterFactory;
import com.risingwave.proto.Data;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.*;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.junit.Test;

public class FlinkMockTest {
    private static final String SINK_V1 = "sinkV1";
    private static final String SINK_V2 = "sinkV2";

    private static MockStorage mockStorage = new MockStorage();

    class MockFlinkMockSinkFactory implements FlinkMockSinkFactory {
        final String mockSinkType;

        public MockFlinkMockSinkFactory(String mockSinkType) {
            this.mockSinkType = mockSinkType;
        }

        @Override
        public void validate(TableSchema tableSchema, FlinkDynamicAdapterConfig config)
                throws StatusRuntimeException {}

        @Override
        public DynamicTableSinkFactory getDynamicTableSinkFactory() {
            return new MockDynamicTableSinkFactory(mockSinkType);
        }
    }

    class MockDynamicTableSinkFactory implements DynamicTableSinkFactory {
        final String mockSinkType;

        public MockDynamicTableSinkFactory(String mockSinkType) {
            this.mockSinkType = mockSinkType;
        }

        @Override
        public DynamicTableSink createDynamicTableSink(Context context) {
            return new MockDynamicTableSink(mockSinkType);
        }

        @Override
        public String factoryIdentifier() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return null;
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return null;
        }
    }

    class MockDynamicTableSink implements DynamicTableSink {
        final String mockSinkType;

        public MockDynamicTableSink(String mockSinkType) {
            this.mockSinkType = mockSinkType;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            if (mockSinkType.equals(SINK_V1)) {
                return SinkProvider.of(new MockSinkV1());
            } else if (mockSinkType.equals(SINK_V2)) {
                return SinkV2Provider.of(new MockSinkV2());
            } else {
                throw new RuntimeException("Don't support sink type");
            }
        }

        @Override
        public DynamicTableSink copy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String asSummaryString() {
            throw new UnsupportedOperationException();
        }
    }

    class MockSinkV1 implements Sink {

        @Override
        public org.apache.flink.api.connector.sink.SinkWriter createWriter(
                InitContext context, List states) throws IOException {
            return new MockSinkWriterV1();
        }

        @Override
        public Optional<SimpleVersionedSerializer> getWriterStateSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Committer> createCommitter() throws IOException {
            return Optional.of(new MockSinkCommitterV1());
        }

        @Override
        public Optional<GlobalCommitter> createGlobalCommitter() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SimpleVersionedSerializer> getCommittableSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SimpleVersionedSerializer> getGlobalCommittableSerializer() {
            throw new UnsupportedOperationException();
        }
    }

    class MockSinkWriterV1 implements SinkWriter {
        List<RowData> cacheList = new ArrayList<>();

        @Override
        public void write(Object element, Context context)
                throws IOException, InterruptedException {
            cacheList.add((RowData) element);
        }

        @Override
        public List prepareCommit(boolean flush) throws IOException, InterruptedException {
            int id = mockStorage.addData(new ArrayList<>(cacheList));
            cacheList.clear();
            return Lists.newArrayList(id);
        }

        @Override
        public void close() throws Exception {}
    }

    class MockSinkCommitterV1 implements Committer {

        public MockSinkCommitterV1() {}

        @Override
        public List commit(List committables) throws IOException, InterruptedException {
            mockStorage.commitId(committables);
            return null;
        }

        @Override
        public void close() throws Exception {}
    }

    class MockSinkV2 implements org.apache.flink.api.connector.sink2.Sink, TwoPhaseCommittingSink {

        @Override
        public PrecommittingSinkWriter createWriter(InitContext context) throws IOException {
            return new MockSinkWriterV2();
        }

        @Override
        public org.apache.flink.api.connector.sink2.Committer createCommitter() throws IOException {
            return null;
        }

        @Override
        public SimpleVersionedSerializer getCommittableSerializer() {
            throw new UnsupportedOperationException();
        }
    }

    class MockSinkWriterV2
            implements org.apache.flink.api.connector.sink2.SinkWriter,
                    TwoPhaseCommittingSink.PrecommittingSinkWriter {
        List<RowData> cacheList = new ArrayList<>();

        List<Integer> ids = new ArrayList<>();

        @Override
        public void write(Object element, Context context)
                throws IOException, InterruptedException {
            cacheList.add((RowData) element);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            int id = mockStorage.addData(new ArrayList<>(cacheList));
            cacheList.clear();
            ids.add(id);
        }

        @Override
        public void close() throws Exception {}

        @Override
        public Collection prepareCommit() throws IOException, InterruptedException {
            mockStorage.commitId(ids);
            return Collections.emptyList();
        }
    }

    static class MockStorage {
        HashMap<Integer, List<RowData>> tempMap;

        int id;

        TreeMap<Integer, List<RowData>> commitMap;

        public MockStorage() {
            tempMap = new HashMap<>();
            id = 0;
            commitMap = new TreeMap<>();
        }

        public int addData(List<RowData> data) {
            int id1 = id++;
            tempMap.put(id1, data);
            return id1;
        }

        public void commitId(List<Integer> ids) {
            for (int id : ids) {
                List<RowData> data = tempMap.remove(id);
                commitMap.put(id, data);
            }
        }
    }

    private TableSchema getTableSchema() {
        return new TableSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(
                        Data.DataType.newBuilder()
                                .setTypeName(Data.DataType.TypeName.INT32)
                                .build(),
                        Data.DataType.newBuilder()
                                .setTypeName(Data.DataType.TypeName.VARCHAR)
                                .build()),
                Lists.newArrayList("id", "name"));
    }

    @Test
    public void testSinkWriteV1() {
        FlinkDynamicAdapterFactory flinkDynamicAdapterFactory =
                new FlinkDynamicAdapterFactory(new MockFlinkMockSinkFactory(SINK_V1));
        com.risingwave.connector.api.sink.SinkWriter writer =
                flinkDynamicAdapterFactory.createWriter(getTableSchema(), new HashMap<>());
        List<SinkRow> sinkRows =
                java.util.Arrays.asList(
                        new ArraySinkRow(Data.Op.INSERT, 1, "Alice"),
                        new ArraySinkRow(Data.Op.INSERT, 2, "Bob"));

        writer.write(sinkRows);
        writer.barrier(true);

        RowData rowData1 = mockStorage.commitMap.lastEntry().getValue().get(0);
        assertEquals(rowData1.getArity(), 2);
        assertEquals(rowData1.getInt(0), 1);
        assertEquals(rowData1.getString(1).toString(), "Alice");

        RowData rowData2 = mockStorage.commitMap.lastEntry().getValue().get(1);
        assertEquals(rowData2.getArity(), 2);
        assertEquals(rowData2.getInt(0), 2);
        assertEquals(rowData2.getString(1).toString(), "Bob");

        List<SinkRow> sinkRows2 =
                java.util.Arrays.asList(
                        new ArraySinkRow(Data.Op.INSERT, 3, "xxx"),
                        new ArraySinkRow(Data.Op.INSERT, 4, "hhh"));

        writer.write(sinkRows2);
        writer.barrier(true);

        RowData rowData3 = mockStorage.commitMap.lastEntry().getValue().get(0);
        assertEquals(rowData3.getArity(), 2);
        assertEquals(rowData3.getInt(0), 3);
        assertEquals(rowData3.getString(1).toString(), "xxx");

        RowData rowData4 = mockStorage.commitMap.lastEntry().getValue().get(1);
        assertEquals(rowData4.getArity(), 2);
        assertEquals(rowData4.getInt(0), 4);
        assertEquals(rowData4.getString(1).toString(), "hhh");
    }

    @Test
    public void testSinkWriteV2() {
        FlinkDynamicAdapterFactory flinkDynamicAdapterFactory =
                new FlinkDynamicAdapterFactory(new MockFlinkMockSinkFactory(SINK_V2));
        com.risingwave.connector.api.sink.SinkWriter writer =
                flinkDynamicAdapterFactory.createWriter(getTableSchema(), new HashMap<>());
        List<SinkRow> sinkRows =
                java.util.Arrays.asList(
                        new ArraySinkRow(Data.Op.INSERT, 1, "Alice"),
                        new ArraySinkRow(Data.Op.INSERT, 2, "Bob"));

        writer.write(sinkRows);
        writer.barrier(true);

        RowData rowData1 = mockStorage.commitMap.lastEntry().getValue().get(0);
        assertEquals(rowData1.getArity(), 2);
        assertEquals(rowData1.getInt(0), 1);
        assertEquals(rowData1.getString(1).toString(), "Alice");

        RowData rowData2 = mockStorage.commitMap.lastEntry().getValue().get(1);
        assertEquals(rowData2.getArity(), 2);
        assertEquals(rowData2.getInt(0), 2);
        assertEquals(rowData2.getString(1).toString(), "Bob");

        List<SinkRow> sinkRows2 =
                java.util.Arrays.asList(
                        new ArraySinkRow(Data.Op.INSERT, 3, "xxx"),
                        new ArraySinkRow(Data.Op.INSERT, 4, "hhh"));

        writer.write(sinkRows2);
        writer.barrier(true);

        RowData rowData3 = mockStorage.commitMap.lastEntry().getValue().get(0);
        assertEquals(rowData3.getArity(), 2);
        assertEquals(rowData3.getInt(0), 3);
        assertEquals(rowData3.getString(1).toString(), "xxx");

        RowData rowData4 = mockStorage.commitMap.lastEntry().getValue().get(1);
        assertEquals(rowData4.getArity(), 2);
        assertEquals(rowData4.getInt(0), 4);
        assertEquals(rowData4.getString(1).toString(), "hhh");
    }
}
