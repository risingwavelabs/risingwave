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

package com.risingwave.connector.sink.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.risingwave.connector.EsSink;
import com.risingwave.connector.EsSinkConfig;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import com.risingwave.proto.Data.Op;
import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class EsSinkTest {

    static TableSchema getTestTableSchema() {
        return new TableSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(
                        Data.DataType.newBuilder().setTypeName(TypeName.INT32).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.VARCHAR).build()),
                Lists.newArrayList("id", "name"));
    }

    public void testEsSink(ElasticsearchContainer container) throws IOException {
        EsSink sink =
                new EsSink(
                        new EsSinkConfig(container.getHttpHostAddress(), "test", "$"),
                        getTestTableSchema());
        sink.write(
                Iterators.forArray(
                        new ArraySinkRow(Op.INSERT, 1, "Alice"),
                        new ArraySinkRow(Op.INSERT, 2, "Bob")));
        sink.sync();
        // container is slow here, but our default flush time is 5s,
        // so 2s is enough for sync test
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }

        RestHighLevelClient client = sink.getClient();
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        assertEquals(2, hits.getHits().length);

        SearchHit hit = hits.getAt(0);
        Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        assertEquals(1, sourceAsMap.get("id"));
        assertEquals("Alice", sourceAsMap.get("name"));
        assertEquals("1$Alice", hit.getId());

        hit = hits.getAt(1);
        sourceAsMap = hit.getSourceAsMap();
        assertEquals(2, sourceAsMap.get("id"));
        assertEquals("Bob", sourceAsMap.get("name"));
        assertEquals("2$Bob", hit.getId());

        sink.drop();
    }

    @Test
    public void testElasticSearch() throws IOException {
        ElasticsearchContainer container =
                new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.11.0");
        container.start();
        testEsSink(container);
        container.stop();
    }
}
