package com.risingwave.pgserver;

import com.google.devtools.common.options.OptionsParser;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.pgwire.PgServer;
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

    Injector injector = Guice.createInjector(new FrontendServerModule(options));

    PgServer server = injector.getInstance(PgServer.class);
    new Thread(server::serve, "PG Server").start();
    LOGGER.info("Frontend server started..");
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: java -jar server.jar OPTIONS");
    System.out.println(
        parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }
}
