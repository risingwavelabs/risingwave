package com.risingwave.connector;

import com.risingwave.connector.api.PkComparator;
import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.util.List;
import java.util.TreeMap;

public class SinkRowMap {
    TreeMap<List<Comparable<Object>>, SinkRowOp> map = new TreeMap<>(new PkComparator());

    public void clear() {
        map.clear();
    }

    public void insert(List<Comparable<Object>> key, SinkRow row) {
        if (!map.containsKey(key)) {
            map.put(key, SinkRowOp.insertOp(row));
        } else {
            SinkRowOp sinkRowOp = map.get(key);
            if (sinkRowOp.isDelete()) {
                map.put(key, SinkRowOp.updateOp(sinkRowOp.getDelete(), row));
            } else {
                throw Status.FAILED_PRECONDITION
                        .withDescription("try to insert a duplicated primary key")
                        .asRuntimeException();
            }
        }
    }

    public void delete(List<Comparable<Object>> key, SinkRow row) {
        if (!map.containsKey(key)) {
            map.put(key, SinkRowOp.deleteOp(row));
        } else {
            SinkRowOp sinkRowOp = map.get(key);
            SinkRow insert = sinkRowOp.getInsert();
            if (insert == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("try to double delete a primary key")
                        .asRuntimeException();
            }
            assertRowValuesEqual(insert, row);
            SinkRow delete = sinkRowOp.getDelete();
            if (delete != null) {
                map.put(key, SinkRowOp.deleteOp(delete));
            } else {
                map.remove(key);
            }
        }
    }

    private void assertRowValuesEqual(SinkRow insert, SinkRow delete) {
        for (int i = 0; i < delete.size(); i++) {
            if (!insert.get(i).equals(delete.get(i))) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                "delete row value "
                                        + delete.get(i)
                                        + " does not match inserted value "
                                        + insert.get(i))
                        .asRuntimeException();
            }
        }
    }
}
