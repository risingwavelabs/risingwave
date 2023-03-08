package com.risingwave.connector;

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
