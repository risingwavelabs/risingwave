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

import com.risingwave.connector.EsSink.RequestTracker;
import org.elasticsearch.action.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkListener
        implements org.elasticsearch.action.bulk.BulkProcessor.Listener,
                org.opensearch.action.bulk.BulkProcessor.Listener {
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    private final RequestTracker requestTracker;

    public BulkListener(RequestTracker requestTracker) {
        this.requestTracker = requestTracker;
    }

    @Override
    public void beforeBulk(long executionId, org.elasticsearch.action.bulk.BulkRequest request) {
        LOG.debug("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
    }

    @Override
    public void afterBulk(
            long executionId,
            org.elasticsearch.action.bulk.BulkRequest request,
            org.elasticsearch.action.bulk.BulkResponse response) {
        if (response.hasFailures()) {
            String errMessage =
                    String.format(
                            "Bulk of %d actions failed. Failure: %s",
                            request.numberOfActions(), response.buildFailureMessage());
            this.requestTracker.addErrResult(errMessage);
        } else {
            this.requestTracker.addOkResult(request.numberOfActions());
            LOG.debug("Sent bulk of {} actions to Elasticsearch.", request.numberOfActions());
        }
    }

    /** This method is called when the bulk failed and raised a Throwable */
    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        String errMessage =
                String.format(
                        "Bulk of %d actions failed. Failure: %s",
                        request.numberOfActions(), failure.getMessage());
        this.requestTracker.addErrResult(errMessage);
    }

    @Override
    public void beforeBulk(long executionId, org.opensearch.action.bulk.BulkRequest request) {
        LOG.debug("Sending bulk of {} actions to Opensearch.", request.numberOfActions());
    }

    @Override
    public void afterBulk(
            long executionId,
            org.opensearch.action.bulk.BulkRequest request,
            org.opensearch.action.bulk.BulkResponse response) {
        if (response.hasFailures()) {
            String errMessage =
                    String.format(
                            "Bulk of %d actions failed. Failure: %s",
                            request.numberOfActions(), response.buildFailureMessage());
            this.requestTracker.addErrResult(errMessage);
        } else {
            this.requestTracker.addOkResult(request.numberOfActions());
            LOG.debug("Sent bulk of {} actions to Opensearch.", request.numberOfActions());
        }
    }

    @Override
    public void afterBulk(
            long executionId, org.opensearch.action.bulk.BulkRequest request, Throwable failure) {
        String errMessage =
                String.format(
                        "Bulk of %d actions failed. Failure: %s",
                        request.numberOfActions(), failure.getMessage());
        this.requestTracker.addErrResult(errMessage);
    }
}
