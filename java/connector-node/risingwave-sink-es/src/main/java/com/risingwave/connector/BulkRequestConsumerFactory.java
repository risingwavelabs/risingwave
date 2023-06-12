package com.risingwave.connector;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.util.function.BiConsumer;

/**
 * {@link BulkRequestConsumerFactory} is used to bridge incompatible Elasticsearch Java API calls
 * across different Elasticsearch versions.
 */
interface BulkRequestConsumerFactory
        extends BiConsumer<BulkRequest, ActionListener<BulkResponse>> {}

