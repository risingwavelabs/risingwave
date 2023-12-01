package com.risingwave.connector.sinkwriter;

import org.apache.flink.api.connector.sink2.Committer;

public class CommitRequestImpl<CommT> implements Committer.CommitRequest<CommT> {
    CommT committable;

    public CommitRequestImpl(CommT committable) {
        this.committable = committable;
    }

    @Override
    public CommT getCommittable() {
        return committable;
    }

    @Override
    public int getNumberOfRetries() {
        return 0;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void retryLater() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAndRetryLater(Object committable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void signalAlreadyCommitted() {
        throw new UnsupportedOperationException();
    }
}
