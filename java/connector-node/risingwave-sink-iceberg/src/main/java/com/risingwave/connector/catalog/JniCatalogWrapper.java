/*
 * Copyright 2024 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.catalog;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Objects;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/** This class provide jni interface to iceberg catalog. */
public class JniCatalogWrapper {
    private final Catalog catalog;

    JniCatalogWrapper(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "Catalog can't be null!");
    }

    /**
     * Load table through this prox.
     *
     * @param tableIdentifier Table identifier.
     * @return Response serialized using json.
     * @throws Exception
     */
    public String loadTable(String tableIdentifier) throws Exception {
        TableIdentifier id = TableIdentifier.parse(tableIdentifier);
        LoadTableResponse resp = CatalogHandlers.loadTable(catalog, id);
        return RESTObjectMapper.mapper().writer().writeValueAsString(resp);
    }

    /**
     * Update table through this prox.
     *
     * @param updateTableRequest Request serialized using json.
     * @return Response serialized using json.
     * @throws Exception
     */
    public String updateTable(String updateTableRequest) throws Exception {
        UpdateTableRequest req =
                RESTObjectMapper.mapper().readValue(updateTableRequest, UpdateTableRequest.class);
        LoadTableResponse resp = CatalogHandlers.updateTable(catalog, req.identifier(), req);
        return RESTObjectMapper.mapper().writer().writeValueAsString(resp);
    }

    /**
     * Create table through this prox.
     *
     * @param namespaceStr String.
     * @param createTableRequest Request serialized using json.
     * @return Response serialized using json.
     * @throws Exception
     */
    public String createTable(String namespaceStr, String createTableRequest) throws Exception {
        Namespace namespace;
        if (namespaceStr == null) {
            namespace = Namespace.empty();
        } else {
            namespace = Namespace.of(namespaceStr);
        }
        CreateTableRequest req =
                RESTObjectMapper.mapper().readValue(createTableRequest, CreateTableRequest.class);
        LoadTableResponse resp = CatalogHandlers.createTable(catalog, namespace, req);
        return RESTObjectMapper.mapper().writer().writeValueAsString(resp);
    }

    /**
     * Checks if a table exists in the catalog.
     *
     * @param tableIdentifier The identifier of the table to check.
     * @return true if the table exists, false otherwise.
     */
    public boolean tableExists(String tableIdentifier) {
        TableIdentifier id = TableIdentifier.parse(tableIdentifier);
        return catalog.tableExists(id);
    }

    /**
     * Create JniCatalogWrapper instance.
     *
     * @param name Catalog name.
     * @param klassName Delegated catalog class name.
     * @param props Catalog properties.
     * @return JniCatalogWrapper instance.
     */
    public static JniCatalogWrapper create(String name, String klassName, String[] props) {
        checkArgument(
                props.length % 2 == 0,
                "props should be key-value pairs, but length is: " + props.length);
        try {
            HashMap<String, String> config = new HashMap<>(props.length / 2);
            for (int i = 0; i < props.length; i += 2) {
                config.put(props[i], props[i + 1]);
            }
            Catalog catalog = CatalogUtil.loadCatalog(klassName, name, config, null);
            return new JniCatalogWrapper(catalog);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
