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

import com.risingwave.connector.api.PkComparator;
import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.util.List;
import java.util.TreeMap;
import org.apache.iceberg.data.Record;

public class SinkRowMap {
    TreeMap<List<Comparable<Object>>, SinkRowOp> map = new TreeMap<>(new PkComparator());

    public void clear() {
        map.clear();
    }

    public void insert(List<Comparable<Object>> key, Record row) {
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

    public void delete(List<Comparable<Object>> key, Record row) {
        if (!map.containsKey(key)) {
            map.put(key, SinkRowOp.deleteOp(row));
        } else {
            SinkRowOp sinkRowOp = map.get(key);
            Record insert = sinkRowOp.getInsert();
            if (insert == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("try to double delete a primary key")
                        .asRuntimeException();
            }
            // TODO: may enable it again
            //            assertRowValuesEqual(insert, row);
            Record delete = sinkRowOp.getDelete();
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
