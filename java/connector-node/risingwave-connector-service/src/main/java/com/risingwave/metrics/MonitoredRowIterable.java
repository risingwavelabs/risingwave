// Copyright 2025 RisingWave Labs
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

public class MonitoredRowIterable implements Iterable<SinkRow> {
    private final Iterable<SinkRow> inner;
    private final String connectorName;
    private final String sinkId;

    public MonitoredRowIterable(Iterable<SinkRow> inner, String connectorName, String sinkId) {
        this.inner = inner;
        this.connectorName = connectorName;
        this.sinkId = sinkId;
    }

    @Override
    public Iterator<SinkRow> iterator() {
        return new MonitoredRowIterator(this.inner.iterator());
    }

    class MonitoredRowIterator implements Iterator<SinkRow> {
        private final Iterator<SinkRow> inner;

        MonitoredRowIterator(Iterator<SinkRow> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public SinkRow next() {
            ConnectorNodeMetrics.incSinkRowsReceived(connectorName, sinkId, 1);
            return inner.next();
        }
    }
}
