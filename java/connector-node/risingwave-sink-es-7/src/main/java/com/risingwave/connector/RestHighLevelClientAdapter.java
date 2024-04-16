package com.risingwave.connector;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.Cancellable;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.core.action.ActionListener;

public interface RestHighLevelClientAdapter {
    public void close() throws IOException;

    public boolean ping(Object options) throws IOException;

    public Object search(Object searchRequest, Object options) throws IOException;
}

class ElasticRestHighLevelClientAdapter implements RestHighLevelClientAdapter {
    org.elasticsearch.client.RestHighLevelClient esClient;

    private static org.elasticsearch.client.RestClientBuilder configureRestClientBuilder(
            org.elasticsearch.client.RestClientBuilder builder, EsSinkConfig config) {
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
                                configureRestClientBuilder(
                                                org.elasticsearch.client.RestClient.builder(host),
                                                config)
                                        .build())
                        .setApiCompatibilityMode(true)
                        .build();
    }

    @Override
    public void close() throws IOException {
        esClient.close();
    }

    @Override
    public boolean ping(Object options) throws IOException {
        boolean flag = esClient.ping((org.elasticsearch.client.RequestOptions) options);
        return flag;
    }

    public org.elasticsearch.client.Cancellable bulkAsync(
            org.elasticsearch.action.bulk.BulkRequest bulkRequest,
            org.elasticsearch.client.RequestOptions options,
            org.elasticsearch.action.ActionListener<org.elasticsearch.action.bulk.BulkResponse>
                    listener) {
        org.elasticsearch.client.Cancellable cancellable =
                esClient.bulkAsync(bulkRequest, options, listener);
        return cancellable;
    }

    @Override
    public Object search(Object searchRequest, Object options) throws IOException {
        return this.esClient.search(
                (org.elasticsearch.action.search.SearchRequest) searchRequest,
                (org.elasticsearch.client.RequestOptions) options);
    }
}

class OpensearchRestHighLevelClientAdapter implements RestHighLevelClientAdapter {
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
                new RestHighLevelClient(
                        configureRestClientBuilder(RestClient.builder(host), config));
    }

    @Override
    public void close() throws IOException {
        opensearchClient.close();
    }

    @Override
    public boolean ping(Object options) throws IOException {
        boolean flag = opensearchClient.ping((RequestOptions) options);
        return flag;
    }

    public Cancellable bulkAsync(
            BulkRequest bulkRequest,
            RequestOptions options,
            ActionListener<BulkResponse> listener) {
        Cancellable cancellable = opensearchClient.bulkAsync(bulkRequest, options, listener);
        return cancellable;
    }

    @Override
    public Object search(Object searchRequest, Object options) throws IOException {
        return this.opensearchClient.search(
                (org.opensearch.action.search.SearchRequest) searchRequest,
                (org.opensearch.client.RequestOptions) options);
    }
}
