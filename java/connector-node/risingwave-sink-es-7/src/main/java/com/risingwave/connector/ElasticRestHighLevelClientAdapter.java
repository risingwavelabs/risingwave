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

package com.risingwave.connector;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;

public class ElasticRestHighLevelClientAdapter implements AutoCloseable {
    RestHighLevelClient esClient;

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, EsSinkConfig config) {
        // Possible config:
        // 1. Connection path prefix
        // 2. Username and password
        if (config.getPassword() != null && config.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            builder.setHttpClientConfigCallback(
                    httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        // 3. Timeout
        return builder;
    }

    public ElasticRestHighLevelClientAdapter(HttpHost host, EsSinkConfig config) {
        this.esClient =
                new RestHighLevelClientBuilder(
                                configureRestClientBuilder(RestClient.builder(host), config)
                                        .build())
                        .setApiCompatibilityMode(true)
                        .build();
    }

    @Override
    public void close() throws IOException {
        esClient.close();
    }

    public boolean ping(RequestOptions options) throws IOException {
        boolean flag = esClient.ping(options);
        return flag;
    }

    public Cancellable bulkAsync(
            BulkRequest bulkRequest,
            RequestOptions options,
            ActionListener<BulkResponse> listener) {
        Cancellable cancellable = esClient.bulkAsync(bulkRequest, options, listener);
        return cancellable;
    }

    public SearchResponse search(SearchRequest searchRequest, RequestOptions options)
            throws IOException {
        return this.esClient.search(searchRequest, options);
    }
}
