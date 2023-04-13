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

package com.risingwave.connector;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.JavaInsertCommitActionExecutor;

public class RisingWaveInsertCommitActionExecutor
        extends JavaInsertCommitActionExecutor<HoodieAvroPayload> {
    public RisingWaveInsertCommitActionExecutor(
            HoodieEngineContext context,
            HoodieWriteConfig config,
            HoodieTable table,
            String instantTime,
            List<HoodieRecord<HoodieAvroPayload>> inputRecords) {
        super(context, config, table, instantTime, inputRecords);
    }

    @Override
    public Iterator<List<WriteStatus>> handleInsert(
            String idPfx, Iterator<HoodieRecord<HoodieAvroPayload>> recordItr) {
        // This is needed since sometimes some buckets are never picked in getPartition() and end up
        // with 0 records
        if (!recordItr.hasNext()) {
            return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
        }
        return new JavaLazyInsertIterable<>(
                recordItr,
                true,
                config,
                instantTime,
                table,
                idPfx,
                taskContextSupplier,
                new RisingWaveCreateHandleFactory());
    }
}
