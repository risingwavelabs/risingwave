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
