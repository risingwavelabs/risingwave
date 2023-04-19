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

package com.risingwave.metrics;

import com.risingwave.connector.api.sink.SinkRow;
import java.util.Iterator;

public class MonitoredRowIterator implements Iterator<SinkRow> {
    private final Iterator<SinkRow> inner;
    private final String sinkType;
    private final String sinkId;

    public MonitoredRowIterator(Iterator<SinkRow> inner, String sinkType, String sinkId) {

        this.inner = inner;
        this.sinkType = sinkType;
        this.sinkId = sinkId;
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public SinkRow next() {
        ConnectorNodeMetrics.incSinkRowsReceived(sinkType, sinkId, 1);
        return inner.next();
    }
}
