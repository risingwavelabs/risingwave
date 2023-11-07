package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

public class FlinkDynamicAdaptSink<CommT> extends SinkWriterBase {
    SinkWriterAdapt sinkWriterAdapt;

    public FlinkDynamicAdaptSink(TableSchema tableSchema, Sink<RowData, CommT, ?, ?> sink) {
        super(tableSchema);
        this.sinkWriterAdapt = new SinkImpl(sink);
    }

    public FlinkDynamicAdaptSink(TableSchema tableSchema, SinkFunction<RowData> sinkFunction) {
        super(tableSchema);
        this.sinkWriterAdapt = new SinkFunctionImpl(sinkFunction);
    }

    public FlinkDynamicAdaptSink(
            TableSchema tableSchema, org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
        super(tableSchema);
        this.sinkWriterAdapt = new SinkV2Impl(sink);
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            sinkWriterAdapt.writer(new RowDataImpl(row, getTableSchema().getNumColumns()));
        }
    }

    @Override
    public void sync() {
        sinkWriterAdapt.commit();
    }

    @Override
    public void drop() {
        sinkWriterAdapt.drop();
    }

    interface SinkWriterAdapt {
        public void commit();

        public void writer(RowData rowData);

        public void abort();

        public void drop();
    }

    class SinkImpl implements SinkWriterAdapt {
        Sink<RowData, CommT, ?, ?> sink;
        org.apache.flink.api.connector.sink.SinkWriter<RowData, ?, ?> writer;

        public SinkImpl(Sink<RowData, CommT, ?, ?> sink) {
            try {
                this.sink = sink;
                this.writer = sink.createWriter(new SinkWriterContext(), Collections.emptyList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void commit() {
            try {
                if (writer == null) {
                    return;
                }
                Optional<? extends Committer<CommT>> committer = sink.createCommitter();
                if (committer.isEmpty()) {
                    return;
                } else {
                    List<CommT> objects = (List<CommT>) writer.prepareCommit(true);
                    committer.get().commit(objects);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                closeWriter();
                this.writer = null;
            }
        }

        @Override
        public void writer(RowData rowData) {
            try {
                if (writer == null) {
                    this.writer =
                            sink.createWriter(new SinkWriterContext(), Collections.emptyList());
                }
                writer.write(
                        rowData,
                        new org.apache.flink.api.connector.sink.SinkWriter.Context() {
                            @Override
                            public long currentWatermark() {
                                return 0;
                            }

                            @Override
                            public Long timestamp() {
                                return null;
                            }
                        });
            } catch (IOException e) {
                closeWriter();
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                closeWriter();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void abort() {
            return;
        }

        private void closeWriter() {
            if (writer == null) {
                return;
            }
            try {
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void drop() {
            closeWriter();
        }
    }

    class SinkFunctionImpl implements SinkWriterAdapt {
        SinkFunction<RowData> sinkFunction;

        public SinkFunctionImpl(SinkFunction<RowData> sinkFunction) {
            this.sinkFunction = sinkFunction;
        }

        @Override
        public void commit() {
            try {
                sinkFunction.finish();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void writer(RowData rowData) {
            try {
                sinkFunction.invoke(
                        rowData,
                        new SinkFunction.Context() {
                            @Override
                            public long currentProcessingTime() {
                                return 0;
                            }

                            @Override
                            public long currentWatermark() {
                                return 0;
                            }

                            @Override
                            public Long timestamp() {
                                return null;
                            }
                        });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void abort() {
            return;
        }

        @Override
        public void drop() {
            return;
        }
    }

    class SinkV2Impl implements SinkWriterAdapt {

        org.apache.flink.api.connector.sink2.Sink<RowData> sink;
        SinkWriter<RowData> writer;

        public SinkV2Impl(org.apache.flink.api.connector.sink2.Sink<RowData> sink) {
            try {
                this.sink = sink;
                this.writer = sink.createWriter(new SinkWriterContextV2());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void commit() {
            if (writer == null) {
                return;
            }
            try {
                this.writer.flush(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println("error 1");
                closeWriter();
                this.writer = null;
            }
        }

        @Override
        public void writer(RowData rowData) {
            System.out.println(this.writer == null);
            try {
                if (this.writer == null) {
                    this.writer = sink.createWriter(new SinkWriterContextV2());
                }
                this.writer.write(
                        rowData,
                        new SinkWriter.Context() {
                            @Override
                            public long currentWatermark() {
                                return 0;
                            }

                            @Override
                            public Long timestamp() {
                                return null;
                            }
                        });
            } catch (IOException e) {
                closeWriter();
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                closeWriter();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void abort() {
            return;
        }

        @Override
        public void drop() {
            closeWriter();
        }

        private void closeWriter() {
            if (writer == null) {
                return;
            }
            try {
                writer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
