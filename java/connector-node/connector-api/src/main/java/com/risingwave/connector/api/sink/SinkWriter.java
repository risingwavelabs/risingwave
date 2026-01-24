/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.api.sink;

import com.risingwave.proto.ConnectorServiceProto;
import java.util.Optional;

public interface SinkWriter {
    /**
     * Begin writing an epoch.
     *
     * @param epoch
     */
    void beginEpoch(long epoch);

    /**
     * Write a series of rows to the external sink.
     *
     * @return Flag to indicate whether the rows are written and persisting in the external sink.
     *     `true` means persisted.
     */
    boolean write(Iterable<SinkRow> rows);

    /**
     * Mark the end of the previous begun epoch.
     *
     * @param isCheckpoint `isCheckpoint` = `true` means that the RW kernel will do a checkpoint for
     *     data before this barrier. External sink should have its data persisted before it returns.
     * @return Optionally return the metadata of this checkpoint. Only return some metadata for
     *     coordinated remote sink when `isCheckpoint` == `true`.
     */
    Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint);

    /** Clean up */
    void drop();
}
