package com.risingwave.common.config;

import static com.risingwave.common.config.Parsers.INT_PARSER;
import static com.risingwave.common.config.Parsers.STRING_PARSER;
import static com.risingwave.common.config.Parsers.enumParserOf;
import static com.risingwave.common.config.Validators.intRangeValidator;

/** FrontendServerConfigurations defined several configurations in frontend server. */
public class FrontendServerConfigurations {
  private FrontendServerConfigurations() {}

  /** Catalog service mode. */
  public enum CatalogMode {
    /** Local means that catalogs stored only in memory without persistence. */
    Local,

    /** Remote stores catalogs in remote meta service. */
    Remote
  }

  @Config
  public static final ConfigEntry<Integer> PG_WIRE_PORT =
      ConfigEntry.<Integer>builder("risingwave.pgserver.port")
          .setOptional(true)
          .withDefaultValue(5432)
          .withDoc("PostgreSQL server port")
          .withConverter(INT_PARSER)
          .withValidator(intRangeValidator(2000, 9000))
          .build();

  @Config
  public static final ConfigEntry<String> PG_WIRE_IP =
      ConfigEntry.<String>builder("risingwave.metadata.ip")
          .setOptional(true)
          .withDefaultValue("0.0.0.0")
          .withDoc("PostgreSQL server IP address")
          .withConverter(STRING_PARSER)
          .build();

  @Config
  public static final ConfigEntry<CatalogMode> CATALOG_MODE =
      ConfigEntry.<CatalogMode>builder("risingwave.catalog.mode")
          .setOptional(false)
          .withDefaultValue(CatalogMode.Local)
          .withDoc("Risingwave's catalog mode")
          .withConverter(enumParserOf(CatalogMode.class))
          .build();

  @Config
  public static final ConfigEntry<String> METADATA_SERVICE_ADDRESS =
      ConfigEntry.<String>builder("risingwave.metadata.node")
          .setOptional(true)
          .withDefaultValue("127.0.0.1:5689")
          .withDoc("Metadata service node address")
          .withConverter(STRING_PARSER)
          .build();
}
