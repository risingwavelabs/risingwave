package com.risingwave.pgserver;

import com.google.devtools.common.options.OptionsParser;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.pgserver.database.RisingWaveDatabaseManager;
import com.risingwave.pgwire.PgServer;
import com.risingwave.pgwire.database.DatabaseManager;
import com.risingwave.pgwire.database.Databases;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrontendServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrontendServer.class);

  public static void main(String[] args) {
    OptionsParser parser = OptionsParser.newOptionsParser(FrontendServerOptions.class);
    parser.parseAndExitUponError(args);
    FrontendServerOptions options = parser.getOptions(FrontendServerOptions.class);
    if (!options.isValid()) {
      printUsage(parser);
      return;
    }

    Configuration config = loadConfig(options);
    CatalogService catalogService = createCatalogService();
    DatabaseManager databaseManager = createDatabaseManager(catalogService, config);

    LOGGER.info("Creating pg server.");
    PgServer server =
        new PgServer(config.get(FrontendServerConfigurations.PG_WIRE_PORT), databaseManager);

    new Thread(server::serve, "PG Server").start();
    LOGGER.info("Frontend server started..");
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: java -jar server.jar OPTIONS");
    System.out.println(
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  private static Configuration loadConfig(FrontendServerOptions options) {
    LOGGER.info("Loading server configuration at {}", options.configFile);
    return Configuration.load(options.configFile, FrontendServerConfigurations.class);
  }

  private static CatalogService createCatalogService() {
    LOGGER.info("Creating catalog service.");
    SimpleCatalogService catalogService = new SimpleCatalogService();
    LOGGER.info("Creating default database: {}.", CatalogService.DEFAULT_DATABASE_NAME);

    catalogService.createDatabase(CatalogService.DEFAULT_DATABASE_NAME);
    return catalogService;
  }

  private static DatabaseManager createDatabaseManager(
      CatalogService catalogService, Configuration configuration) {
    LOGGER.info("Creating database manager.");
    DatabaseManager manager = new RisingWaveDatabaseManager(configuration, catalogService);
    Databases.setDatabaseManager(manager);
    return manager;
  }
}
