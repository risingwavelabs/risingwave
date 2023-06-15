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

import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.util.HashMap;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;

public class HudiSinkRowMap {
    HashMap<HoodieKey, HudiSinkRowOp> map = new HashMap<>();

    public void clear() {
        map.clear();
    }

    public void insert(HoodieAvroRecord<HoodieAvroPayload> row) {
        if (!map.containsKey(row.getKey())) {
            map.put(row.getKey(), HudiSinkRowOp.insertOp(row));
        } else {
            HudiSinkRowOp sinkRowOp = map.get(row.getKey());
            if (sinkRowOp.isDelete()) {
                map.put(row.getKey(), HudiSinkRowOp.updateOp(row));
            } else {
                throw Status.FAILED_PRECONDITION
                        .withDescription("try to insert a duplicated primary key")
                        .asRuntimeException();
            }
        }
    }

    public void delete(HoodieKey key) {
        if (!map.containsKey(key)) {
            map.put(key, HudiSinkRowOp.deleteOp(key));
        } else {
            HudiSinkRowOp sinkRowOp = map.get(key);
            HoodieAvroRecord<HoodieAvroPayload> insert = sinkRowOp.getInsert();
            if (insert == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("try to double delete a primary key")
                        .asRuntimeException();
            }
            // TODO: may enable it again
            //            assertRowValuesEqual(insert, row);
            HoodieAvroRecord<HoodieAvroPayload> delete = sinkRowOp.getDelete();
            if (delete != null) {
                map.put(key, HudiSinkRowOp.deleteOp(key));
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
