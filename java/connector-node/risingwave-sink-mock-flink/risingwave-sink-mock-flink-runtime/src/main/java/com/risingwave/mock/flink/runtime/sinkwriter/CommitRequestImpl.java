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

package com.risingwave.mock.flink.runtime.sinkwriter;

import org.apache.flink.api.connector.sink2.Committer;

/**
 * The mock implementation of CommitRequest. Rw don't support two-stage committed, so there is no
 * need to simply return Committable
 */
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
