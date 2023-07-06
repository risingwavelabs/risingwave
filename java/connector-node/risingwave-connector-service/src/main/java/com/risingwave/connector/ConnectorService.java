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

package com.risingwave.connector;

import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.metrics.ConnectorNodeMetrics;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorService {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorService.class);
    static final int DEFAULT_PORT = 50051;
    static final int DEFAULT_PROMETHEUS_PORT = 50052;
    static final String PORT_ENV_NAME = "RW_CONNECTOR_NODE_PORT";
    static final String PROMETHEUS_PORT_ENV_NAME = "RW_CONNECTOR_NODE_PROMETHEUS_PORT";

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("p", "port", true, "listening port of connector service");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // Quoted from the debezium document:
        // > Your application should always properly stop the engine to ensure graceful and complete
        // > shutdown and that each source record is sent to the application exactly one time.
        // However, in RisingWave we assume the upstream changelog may contain duplicate events and
        // handle conflicts in the mview operator, thus we don't need to obey the above
        // instructions.
        // So we decrease the wait time to 1 second here to reclaim grpc resources faster when the
        // grpc channel is broken.
        System.setProperty(DbzConnectorConfig.WAIT_FOR_CONNECTOR_EXIT_BEFORE_INTERRUPT_MS, "1000");

        int port = DEFAULT_PORT;
        if (cmd.hasOption("p")) {
            var portVal = cmd.getOptionValue("p");
            port = Integer.parseInt(portVal);
        } else if (System.getenv().containsKey(PORT_ENV_NAME)) {
            port = Integer.parseInt(System.getenv(PORT_ENV_NAME));
        }
        Server server =
                ServerBuilder.forPort(port).addService(new ConnectorServiceImpl()).build().start();
        LOG.info("Server started, listening on {}", server.getPort());

        int prometheusPort = DEFAULT_PROMETHEUS_PORT;
        if (System.getenv().containsKey(PROMETHEUS_PORT_ENV_NAME)) {
            prometheusPort = Integer.parseInt(System.getenv(PROMETHEUS_PORT_ENV_NAME));
        }
        ConnectorNodeMetrics.startHTTPServer(prometheusPort);
        LOG.info("Prometheus metrics server started, listening on {}", prometheusPort);
        server.awaitTermination();
    }
}
