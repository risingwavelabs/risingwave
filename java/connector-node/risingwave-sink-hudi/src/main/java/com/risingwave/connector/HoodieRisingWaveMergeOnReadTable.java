package com.risingwave.connector;

import java.util.List;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieJavaMergeOnReadTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

public class HoodieRisingWaveMergeOnReadTable
        extends HoodieJavaMergeOnReadTable<HoodieAvroPayload> {
    protected HoodieRisingWaveMergeOnReadTable(
            HoodieWriteConfig config,
            HoodieEngineContext context,
            HoodieTableMetaClient metaClient) {
        super(config, context, metaClient);
        this.validateUpsertSchema();
    }

    public static HoodieRisingWaveMergeOnReadTable create(
            HoodieWriteConfig config,
            HoodieEngineContext context,
            HoodieTableMetaClient metaClient) {
        return new HoodieRisingWaveMergeOnReadTable(config, context, metaClient);
    }

    @Override
    public HoodieWriteMetadata<List<WriteStatus>> insert(
            HoodieEngineContext context,
            String instantTime,
            List<HoodieRecord<HoodieAvroPayload>> records) {
        return new RisingWaveInsertCommitActionExecutor(context, config, this, instantTime, records)
                .execute();
    }

    @Override
    public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(
            HoodieEngineContext context,
            String instantTime,
            List<HoodieRecord<HoodieAvroPayload>> preppedRecords) {
        return new RisingWaveUpsertPreppedDeltaCommitActionExecutor(
                        (HoodieJavaEngineContext) context,
                        config,
                        this,
                        instantTime,
                        preppedRecords)
                .execute();
    }
}
