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

package com.risingwave.connector.common;

import static io.grpc.Status.INVALID_ARGUMENT;

import org.apache.hadoop.conf.Configuration;

public class S3Utils {

    private static final String confEndpoint = "fs.s3a.endpoint";
    private static final String confKey = "fs.s3a.access.key";
    private static final String confSecret = "fs.s3a.secret.key";
    private static final String confPathStyleAccess = "fs.s3a.path.style.access";

    public static Configuration getHadoopConf(S3Config config) {
        Configuration hadoopConf = new Configuration();
        hadoopConf.setBoolean(confPathStyleAccess, true);
        if (!config.hasS3Endpoint()) {
            throw INVALID_ARGUMENT
                    .withDescription(String.format("Should set `%s`", confEndpoint))
                    .asRuntimeException();
        }
        hadoopConf.set(confEndpoint, config.getS3Endpoint());
        if (!config.hasS3AccessKey()) {
            throw INVALID_ARGUMENT
                    .withDescription(String.format("Should set `%s`", confKey))
                    .asRuntimeException();
        }
        hadoopConf.set(confKey, config.getS3AccessKey());
        if (!config.hasS3SecretKey()) {
            throw INVALID_ARGUMENT
                    .withDescription(String.format("Should set `%s`", confSecret))
                    .asRuntimeException();
        }
        hadoopConf.set(confSecret, config.getS3SecretKey());
        return hadoopConf;
    }
}
