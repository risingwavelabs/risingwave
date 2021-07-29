package com.risingwave.common.config;

import static com.risingwave.common.config.Converters.INT_CONVERTER;
import static com.risingwave.common.config.Validators.intRangeValidator;

public class FrontendServerConfigurations {
  @Config
  public static final ConfigEntry<Integer> PG_WIRE_PORT =
      ConfigEntry.<Integer>builder("risingwave.pgserver.port")
          .setOptional(true)
          .withDefaultValue(5432)
          .withDoc("Postgre sql server port")
          .withConverter(INT_CONVERTER)
          .withValidator(intRangeValidator(2000, 9000))
          .build();

  @Config
  public static final ConfigEntry<Integer> COMPUTE_NODE_SERVER_PORT =
      ConfigEntry.<Integer>builder("risingwave.computenode.port")
          .setOptional(true)
          .withDefaultValue(5433)
          .withDoc("Compute node server port")
          .withConverter(INT_CONVERTER)
          .withValidator(intRangeValidator(2000, 9000))
          .build();
}
