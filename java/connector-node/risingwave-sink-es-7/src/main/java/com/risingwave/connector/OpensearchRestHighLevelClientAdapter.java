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
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.Cancellable;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.core.action.ActionListener;

public class OpensearchRestHighLevelClientAdapter implements AutoCloseable {
    RestHighLevelClient opensearchClient;

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

    public OpensearchRestHighLevelClientAdapter(HttpHost host, EsSinkConfig config) {
        this.opensearchClient =
                new org.opensearch.client.RestHighLevelClient(
                        configureRestClientBuilder(
                                org.opensearch.client.RestClient.builder(host), config));
    }

    @Override
    public void close() throws IOException {
        opensearchClient.close();
    }

    public boolean ping(org.opensearch.client.RequestOptions options) throws IOException {
        boolean flag = opensearchClient.ping(options);
        return flag;
    }

    public Cancellable bulkAsync(
            BulkRequest bulkRequest,
            RequestOptions options,
            ActionListener<BulkResponse> listener) {
        Cancellable cancellable = opensearchClient.bulkAsync(bulkRequest, options, listener);
        return cancellable;
    }
}
