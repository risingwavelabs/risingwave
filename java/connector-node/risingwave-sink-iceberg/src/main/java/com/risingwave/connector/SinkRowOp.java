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

import io.grpc.Status;
import org.apache.iceberg.data.Record;

public class SinkRowOp {
    private final Record delete;
    private final Record insert;

    public static SinkRowOp insertOp(Record row) {
        if (row == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row op must not be null to initialize insertOp")
                    .asRuntimeException();
        }
        return new SinkRowOp(null, row);
    }

    public static SinkRowOp deleteOp(Record row) {
        if (row == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row op must not be null to initialize deleteOp")
                    .asRuntimeException();
        }
        return new SinkRowOp(row, null);
    }

    public static SinkRowOp updateOp(Record delete, Record insert) {
        if (delete == null || insert == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row ops must not be null initialize updateOp")
                    .asRuntimeException();
        }
        return new SinkRowOp(delete, insert);
    }

    private SinkRowOp(Record delete, Record insert) {
        this.delete = delete;
        this.insert = insert;
    }

    public boolean isDelete() {
        return insert == null && delete != null;
    }

    public Record getDelete() {
        return delete;
    }

    public Record getInsert() {
        return insert;
    }
}
