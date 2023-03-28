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
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;

// TODO: refactor the code to unify it with SinkRowOp in iceberg
public class HudiSinkRowOp {
    private final HoodieAvroRecord<HoodieAvroPayload> delete;
    private final HoodieAvroRecord<HoodieAvroPayload> insert;

    public static HudiSinkRowOp insertOp(HoodieAvroRecord<HoodieAvroPayload> row) {
        if (row == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row op must not be null to initialize insertOp")
                    .asRuntimeException();
        }
        return new HudiSinkRowOp(null, row);
    }

    public static HudiSinkRowOp deleteOp(HoodieKey key) {
        return new HudiSinkRowOp(
                new HoodieAvroRecord(key, new HoodieAvroPayload(Option.empty())), null);
    }

    public static HudiSinkRowOp updateOp(HoodieAvroRecord<HoodieAvroPayload> insert) {
        return new HudiSinkRowOp(
                new HoodieAvroRecord(insert.getKey(), new HoodieAvroPayload(Option.empty())),
                insert);
    }

    private HudiSinkRowOp(
            HoodieAvroRecord<HoodieAvroPayload> delete,
            HoodieAvroRecord<HoodieAvroPayload> insert) {
        this.delete = delete;
        this.insert = insert;
    }

    public boolean isDelete() {
        return insert == null && delete != null;
    }

    public HoodieAvroRecord<HoodieAvroPayload> getDelete() {
        return delete;
    }

    public HoodieAvroRecord<HoodieAvroPayload> getInsert() {
        return insert;
    }
}
