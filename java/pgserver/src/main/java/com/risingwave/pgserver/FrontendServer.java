package com.risingwave.pgserver;

import com.google.devtools.common.options.OptionsParser;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.pgwire.PgServer;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RisingWave database entry will start the PostgreSql process on the port in the configuration
 * file.
 */
public class FrontendServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrontendServer.class);

  public static void main(String[] args) {
    OptionsParser parser = OptionsParser.newOptionsParser(FrontendServerOptions.class);
    parser.parseAndExitUponError(args);
    FrontendServerOptions options = parser.getOptions(FrontendServerOptions.class);
    assert options != null;
    if (!options.isValid()) {
      printUsage(parser);
      return;
    }

    var frontendMod = new FrontendServerModule(options);
    Injector injector = Guice.createInjector(frontendMod);

    PgServer server = injector.getInstance(PgServer.class);
    new Thread(server::serve, "PG Server").start();
    LOGGER.info("Frontend server started: {}", frontendMod.systemConfig().toString());
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: java -jar risingwave-fe-runnable.jar OPTIONS");
    System.out.println(
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }
}
