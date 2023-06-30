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

import static io.grpc.Status.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.common.S3Utils;
import com.risingwave.java.utils.UrlParser;
import com.risingwave.proto.Catalog.SinkType;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.types.StructType;
import io.grpc.Status;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class DeltaLakeSinkFactory implements SinkFactory {
    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        DeltaLakeSinkConfig config =
                mapper.convertValue(tableProperties, DeltaLakeSinkConfig.class);

        Configuration hadoopConf = getConfig(config.getLocation(), config);

        DeltaLog log = DeltaLog.forTable(hadoopConf, config.getLocation());
        StructType schema = log.snapshot().getMetadata().getSchema();
        DeltaLakeSinkUtil.checkSchema(tableSchema, schema);
        return new DeltaLakeSink(tableSchema, hadoopConf, log);
    }

    @Override
    public void validate(
            TableSchema tableSchema, Map<String, String> tableProperties, SinkType sinkType) {
        if (sinkType != SinkType.APPEND_ONLY && sinkType != SinkType.FORCE_APPEND_ONLY) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("only append-only delta lake sink is supported")
                    .asRuntimeException();
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
        DeltaLakeSinkConfig config =
                mapper.convertValue(tableProperties, DeltaLakeSinkConfig.class);

        String location = config.getLocation();

        Configuration hadoopConf = getConfig(config.getLocation(), config);

        DeltaLog log = DeltaLog.forTable(hadoopConf, location);
        StructType schema = log.snapshot().getMetadata().getSchema();
        DeltaLakeSinkUtil.checkSchema(tableSchema, schema);
        DeltaLakeSinkUtil.convertSchema(log, tableSchema);
    }

    private Configuration getConfig(String location, DeltaLakeSinkConfig config) {
        String scheme = UrlParser.parseLocationScheme(location);
        switch (scheme) {
            case "file":
                return new Configuration();
            case "s3":
            case "s3a":
                return S3Utils.getHadoopConf(config);
            default:
                throw UNIMPLEMENTED
                        .withDescription(
                                String.format(
                                        "unsupported deltalake sink location scheme: %s", scheme))
                        .asRuntimeException();
        }
    }
}
