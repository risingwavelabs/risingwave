package com.risingwave.connector;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.HoodieWriteMetadata;

public class HoodieRisingWaveWriter extends HoodieJavaWriteClient<HoodieAvroPayload> {
    public static final String RISINGWAVE_FILE_ID = "risingwave-file-id";
    private static final HoodieRecordLocation RISINGWAVE_LOCATION =
            new HoodieRecordLocation(null, RISINGWAVE_FILE_ID);

    private final HoodieEngineContext context;
    private final HoodieWriteConfig writeConfig;

    public HoodieRisingWaveWriter(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
        super(context, writeConfig);
        this.context = context;
        this.writeConfig = writeConfig;
    }

    public void ingest(List<HoodieRecord<HoodieAvroPayload>> records, String instantTime) {
        // We should recreate a new table meta client in every commit so that it can read the latest
        HoodieRisingWaveMergeOnReadTable table =
                HoodieRisingWaveMergeOnReadTable.create(
                        this.writeConfig,
                        this.context,
                        loadTableMetaClient(writeConfig.getBasePath(), hadoopConf));
        // TODO: use partition path to get latest base files
        List<HoodieBaseFile> baseFiles =
                table.getBaseFileOnlyView().getLatestBaseFiles("").collect(Collectors.toList());
        if (baseFiles.size() == 0) {
            // Do insert to create a file id with name risingwave
            //            preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
            HoodieWriteMetadata<List<WriteStatus>> result =
                    table.insert(context, instantTime, records);
            //            postWrite(result, instantTime, table);
        } else {
            if (baseFiles.size() > 1) {
                throw new RuntimeException("should not contain more than 2 file id");
            }
            if (!baseFiles.get(0).getFileId().equals(RISINGWAVE_FILE_ID)) {
                throw new RuntimeException(
                        "invalid valid file id for risingwave " + baseFiles.get(0).getFileId());
            }
            records.forEach(
                    record -> {
                        record.unseal();
                        record.setCurrentLocation(RISINGWAVE_LOCATION);
                        record.seal();
                    });
            // Do upsert
            table.upsertPrepped(context, instantTime, records);
        }
    }

    static HoodieTableMetaClient loadTableMetaClient(String basePath, Configuration hadoopConf) {
        return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
    }
}
