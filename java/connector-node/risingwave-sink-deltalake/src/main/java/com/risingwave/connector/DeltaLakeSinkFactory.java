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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.java.utils.MinioUrlParser;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.types.StructType;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class DeltaLakeSinkFactory implements SinkFactory {

    private static final String LOCATION_PROP = "location";
    private static final String LOCATION_TYPE_PROP = "location.type";
    private static final String confEndpoint = "fs.s3a.endpoint";
    private static final String confKey = "fs.s3a.access.key";
    private static final String confSecret = "fs.s3a.secret.key";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        String location = tableProperties.get(LOCATION_PROP);
        String locationType = tableProperties.get(LOCATION_TYPE_PROP);

        Configuration hadoopConf = new Configuration();
        location = getConfig(location, locationType, hadoopConf);

        DeltaLog log = DeltaLog.forTable(hadoopConf, location);
        StructType schema = log.snapshot().getMetadata().getSchema();
        DeltaLakeSinkUtil.checkSchema(tableSchema, schema);
        return new DeltaLakeSink(tableSchema, hadoopConf, log);
    }

    @Override
    public TableSchema validate(TableSchema tableSchema, Map<String, String> tableProperties) {
        if (!tableProperties.containsKey(LOCATION_PROP)
                || !tableProperties.containsKey(LOCATION_TYPE_PROP)) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "%s or %s is not specified", LOCATION_PROP, LOCATION_TYPE_PROP))
                    .asRuntimeException();
        }

        String location = tableProperties.get(LOCATION_PROP);
        String locationType = tableProperties.get(LOCATION_TYPE_PROP);

        Configuration hadoopConf = new Configuration();
        location = getConfig(location, locationType, hadoopConf);

        DeltaLog log = DeltaLog.forTable(hadoopConf, location);
        StructType schema = log.snapshot().getMetadata().getSchema();
        DeltaLakeSinkUtil.checkSchema(tableSchema, schema);
        DeltaLakeSinkUtil.convertSchema(log, tableSchema);

        return tableSchema;
    }

    private String getConfig(String location, String locationType, Configuration hadoopConf) {
        switch (locationType) {
            case "local":
                location = "file://" + Paths.get(location).toAbsolutePath();
                break;
            case "s3":
                location = "s3a:" + location.substring(location.indexOf('/'));
                break;
            case "minio":
                MinioUrlParser minioUrlParser = new MinioUrlParser(location);
                hadoopConf.set(confEndpoint, minioUrlParser.getEndpoint());
                hadoopConf.set(confKey, minioUrlParser.getKey());
                hadoopConf.set(confSecret, minioUrlParser.getSecret());
                location = "s3a://" + minioUrlParser.getBucket();
                break;
            default:
                throw UNIMPLEMENTED
                        .withDescription("unsupported deltalake sink type: " + locationType)
                        .asRuntimeException();
        }
        return location;
    }
}
