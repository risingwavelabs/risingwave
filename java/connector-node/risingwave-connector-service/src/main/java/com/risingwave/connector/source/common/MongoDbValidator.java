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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbValidator extends DatabaseValidator {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDbValidator.class);

    String mongodbUrl;
    boolean isAuthEnabled;
    boolean isShardedCluster;

    ConnectionString connStr;
    MongoClient client;
    final static String USERs = "users";
    final static String ROLES = "roles";
    final static String INHERITED_ROLES = "inheritedRoles";
    final static String INHERITED_PRIVILEGES = "inheritedPrivileges";


    public MongoDbValidator(Map<String, String> userProps) {
        this.mongodbUrl = userProps.get("mongodb.url");
        // TODO: check
        this.connStr = new ConnectionString(mongodbUrl);
        this.isAuthEnabled = false;
        this.isShardedCluster = false;
        this.client = MongoClients.create(connStr.toString());
    }

    @Override
    public void validateDbConfig() {
        // check connectivity
        try (MongoClient mongoClient = MongoClients.create(mongodbUrl)) {
            var desc = mongoClient.getClusterDescription();
            LOG.info("test connectivity: MongoDB cluster description: {}", desc);
        }
    }

    boolean checkReadRoleForAdminDb(List<Document> roles) {
        for (Document roleDoc : roles) {
            var db = roleDoc.getString("db");
            var role = roleDoc.getString("role");
            if (db.equals("admin") && (role.equals("readWrite") || role.equals("read"))) {
                LOG.info("user has the appropriate roles to read the admin database");
                return true;
            }
        }
        return false;
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

        if (null != connStr.getCredential()) {
            // TODO: user is provided, check user priviledge
            var secret = connStr.getCredential();

            var authDb = client.getDatabase(secret.getSource());

            Bson command =
                    BsonDocument.parse(
                            String.format(
                                    "{usersInfo: \"%s\", showPrivileges: true}",
                                    secret.getUserName()));

            Document ret = authDb.runCommand(command);
            LOG.info("userInfo: {}", ret.toJson());

            List<Document> users = ret.getEmbedded(List.of(USERs), List.class);
            LOG.info("mongodb users => {}", users);
            if (users.isEmpty()) {
                throw new CdcConnectorException("user not found in the database");
            }

            Document user = users.get(0);
            // check roles
            List<Document> roles = user.getEmbedded(List.of(ROLES), List.class);
            boolean hasReadForAdmin = false;
            if (!roles.isEmpty()) {
                // check direct roles
                hasReadForAdmin = checkReadRoleForAdminDb(roles);
                if (!hasReadForAdmin) {
                    // check inherited roles
                    List<Document> inheriRoles = user.getEmbedded(List.of(INHERITED_ROLES), List.class);
                    if (!inheriRoles.isEmpty()) {
                        hasReadForAdmin = checkReadRoleForAdminDb(inheriRoles);
                    }
                }
            }
            if (!hasReadForAdmin) {
                throw new CdcConnectorException(
                        "user does not have the appropriate roles to read the admin database");
            }

            // When change streams are used (the default) the user also
            // must have cluster-wide privilege actions find and changeStream.
            List<Document> inheriPrivis = user.getEmbedded(List.of(INHERITED_PRIVILEGES), List.class);
            if (!inheriPrivis.isEmpty()) {
                for (Document privi : inheriPrivis) {
                    // TODO:
                }
            }
        }

        if (isShardedCluster) {
            // TODO: user must able to read the config database
            // the user must also be able to read the config database in the configuration server of a sharded cluster
            // and must have listDatabases privilege action.
        }
    }

    @Override
    void validateTable() {
        // do nothing since MongoDB is schemaless
    }
}
