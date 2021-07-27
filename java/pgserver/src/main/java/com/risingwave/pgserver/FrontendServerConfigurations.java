package com.risingwave.pgserver;

import static com.risingwave.common.config.Converters.INT_CONVERTER;
import static com.risingwave.common.config.Validators.intRangeValidator;

import com.risingwave.common.config.Config;
import com.risingwave.common.config.ConfigEntry;

public class FrontendServerConfigurations {
  @Config
  public static final ConfigEntry<Integer> SERVER_PORT =
      ConfigEntry.<Integer>builder("risingwave.pgserver.port")
          .setOptional(true)
          .withDefaultValue(5432)
          .withDoc("Postgre sql server port")
          .withConverter(INT_CONVERTER)
          .withValidator(intRangeValidator(2000, 9000))
          .build();
}
