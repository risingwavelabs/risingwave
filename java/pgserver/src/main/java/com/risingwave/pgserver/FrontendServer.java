package com.risingwave.pgserver;

import com.google.devtools.common.options.OptionsParser;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
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

    LOGGER.atInfo().log("Creating pg server.");
    PgServer server =
        new PgServer(config.get(FrontendServerConfigurations.SERVER_PORT), databaseManager);

    new Thread(server::serve, "PG Server").start();
    LOGGER.atInfo().log("Frontend server started..");
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: java -jar server.jar OPTIONS");
    System.out.println(
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  private static Configuration loadConfig(FrontendServerOptions options) {
    LOGGER.atInfo().addArgument(options.configFile).log("Loading server configuration at {}");
    return Configuration.load(options.configFile, FrontendServerConfigurations.class);
  }

  private static CatalogService createCatalogService() {
    LOGGER.atInfo().log("Creating catalog service.");
    SimpleCatalogService catalogService = new SimpleCatalogService();
    LOGGER
        .atInfo()
        .addArgument(CatalogService.DEFAULT_DATABASE_NAME)
        .log("Creating default database: {}.", CatalogService.DEFAULT_DATABASE_NAME);

    catalogService.createDatabase(CatalogService.DEFAULT_DATABASE_NAME);
    return catalogService;
  }

  private static DatabaseManager createDatabaseManager(
      CatalogService catalogService, Configuration configuration) {
    LOGGER.atInfo().log("Creating database manager.");
    DatabaseManager manager = new RisingWaveDatabaseManager(configuration, catalogService);
    Databases.setDatabaseManager(manager);
    return manager;
  }
}
