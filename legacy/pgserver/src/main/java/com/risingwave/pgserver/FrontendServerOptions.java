package com.risingwave.pgserver;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

public class FrontendServerOptions extends OptionsBase {
  @Option(name = "help", abbrev = 'h', help = "Prints usage info.", defaultValue = "true")
  public boolean help;

  @Option(
      name = "config",
      abbrev = 'c',
      help = "Configuration file path.",
      category = "startup",
      defaultValue = "")
  public String configFile;

  @Option(
      name = "combine-leader",
      help = "Whether to combine the leader's functionalities into the frontend server",
      category = "startup",
      defaultValue = "true")
  public boolean combineLeader;

  public boolean isValid() {
    return configFile != null && !configFile.isEmpty();
  }
}
