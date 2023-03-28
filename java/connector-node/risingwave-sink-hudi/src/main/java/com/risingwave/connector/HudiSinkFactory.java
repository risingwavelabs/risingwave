package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.Catalog;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;

public class HudiSinkFactory implements SinkFactory {
    static final String BASE_PATH_PROP = "base.path";
    static final String TABLE_NAME_PROP = "table.name";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        return new HudiSink(
                tableProperties.get(BASE_PATH_PROP),
                tableProperties.get(TABLE_NAME_PROP),
                getHadoopConf(tableProperties),
                tableSchema);
    }

    @Override
    public void validate(
            TableSchema tableSchema,
            Map<String, String> tableProperties,
            Catalog.SinkType sinkType) {

        // TODO: check config exists
        String basePath = tableProperties.get(BASE_PATH_PROP);
        Configuration hadoopConf = new Configuration();

        HoodieTableMetaClient client = loadTableMetaClient(basePath, hadoopConf);
        // TODO: check whether the table is merge on read
    }

    static HoodieTableMetaClient loadTableMetaClient(String basePath, Configuration hadoopConf) {
        return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
    }

    static Configuration getHadoopConf(Map<String, String> tableProperties) {
        // TODO: set S3 config
        return new Configuration();
    }
}
