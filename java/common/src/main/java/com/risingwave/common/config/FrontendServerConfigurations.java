package com.risingwave.common.config;

import static com.risingwave.common.config.Parsers.INT_PARSER;
import static com.risingwave.common.config.Validators.intRangeValidator;

public class FrontendServerConfigurations {
  private FrontendServerConfigurations() {}

  @Config
  public static final ConfigEntry<Integer> PG_WIRE_PORT =
      ConfigEntry.<Integer>builder("risingwave.pgserver.port")
          .setOptional(true)
          .withDefaultValue(5432)
          .withDoc("Postgre sql server port")
          .withConverter(INT_PARSER)
          .withValidator(intRangeValidator(2000, 9000))
          .build();
}
