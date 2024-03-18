// Copyright 2024 RisingWave Labs
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

package com.risingwave.connector.source.common;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbValidator extends DatabaseValidator {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDbValidator.class);

    String mongodbUrl;

    public MongoDbValidator(Map<String, String> userProps) {
        this.mongodbUrl = userProps.get("mongodb.url");
    }

    @Override
    public void validateDbConfig() {
        // check connectivity
        try (MongoClient mongoClient = MongoClients.create(mongodbUrl)) {
            var desc = mongoClient.getClusterDescription();
            LOG.info("test connectivity: MongoDB cluster description: {}", desc);
        }
    }

    @Override
    void validateUserPrivilege() {
        // TODO: check user privilege
        // https://debezium.io/documentation/reference/stable/connectors/mongodb.html#setting-up-mongodb
        // You must also have a MongoDB user that has the appropriate roles to read the admin
        // database where the oplog can be read. Additionally, the user must also be able to read
        // the config database in the configuration server of a sharded cluster and must have
        // listDatabases privilege action. When change streams are used (the default) the user also
        // must have cluster-wide privilege actions find and changeStream.
    }

    @Override
    void validateTable() {
        // do nothing since MongoDB is schemaless
    }

    @Override
    boolean isCdcSourceJob() {
        return false;
    }
}
