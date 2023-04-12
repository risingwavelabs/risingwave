package com.risingwave.connector;

import static io.grpc.Status.UNIMPLEMENTED;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.common.S3Utils;
import com.risingwave.java.utils.UrlParser;
import com.risingwave.proto.Catalog;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;

public class HudiSinkFactory implements SinkFactory {

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        HudiSinkConfig config = mapper.convertValue(tableProperties, HudiSinkConfig.class);

        return new HudiSink(
                config.getBasePath(), config.getTableName(), getHadoopConf(config), tableSchema);
    }

    @Override
    public void validate(
            TableSchema tableSchema,
            Map<String, String> tableProperties,
            Catalog.SinkType sinkType) {

        ObjectMapper mapper = new ObjectMapper();
        HudiSinkConfig config = mapper.convertValue(tableProperties, HudiSinkConfig.class);

        // TODO: check config exists
        Configuration hadoopConf = getHadoopConf(config);

        HoodieTableMetaClient client = loadTableMetaClient(config.getBasePath(), hadoopConf);
        // TODO: check whether the table is merge on read
    }

    static HoodieTableMetaClient loadTableMetaClient(String basePath, Configuration hadoopConf) {
        return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
    }

    static Configuration getHadoopConf(HudiSinkConfig config) {
        String scheme = UrlParser.parseLocationScheme(config.getBasePath());
        switch (scheme) {
            case "file":
                return new Configuration();
            case "s3":
            case "s3a":
                return S3Utils.getHadoopConf(config);
            default:
                throw UNIMPLEMENTED
                        .withDescription(
                                String.format("scheme %s not supported for base path", scheme))
                        .asRuntimeException();
        }
    }
}
