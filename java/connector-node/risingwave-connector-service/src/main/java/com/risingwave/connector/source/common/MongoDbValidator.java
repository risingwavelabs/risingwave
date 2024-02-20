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
            LOG.info("MongoDB cluster description: {}", desc);
        }
    }

    @Override
    void validateUserPrivilege() {}

    @Override
    void validateTable() {
        // do nothing
    }
}
