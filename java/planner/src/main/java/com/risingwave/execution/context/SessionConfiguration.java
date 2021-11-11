package com.risingwave.execution.context;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.common.config.ConfigEntry;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage session level config values. A parameter look up will get in local config map, if not set
 * before, get default value from global config registry.
 */
public class SessionConfiguration {

  /** String key -> config value (T). Session level configurations. */
  private final Map<String, Object> confData = new HashMap<>();

  private final Configuration globalConf;

  public SessionConfiguration(Configuration globalConf) {
    this.globalConf = globalConf;
  }

  /** Set config value using string key and string value. Used in set parameter handler. */
  public <T> void setByString(String stringKey, String value) {
    checkNotNull(value, "Value can't be null!");
    // Make sure the parameter is defined.
    getByString(stringKey);
    confData.put(stringKey, globalConf.getConfigEntry(stringKey).getParser().convert(value));
  }

  /** Get config value by using the string key. Used in show parameter handler. */
  public <T> T getByString(String stringKey) {
    Object rawValue = confData.get(stringKey);
    if (rawValue == null) {
      // Find default value in config registry.
      ConfigEntry<?> entry = globalConf.getConfigEntry(stringKey);
      if (entry == null) {
        throw new PgException(
            PgErrorCode.CONFIG_FILE_ERROR,
            "Config %s is missing and has no default value!",
            stringKey);
      } else {
        return (T) entry.getDefaultValue();
      }
    } else {
      return (T) rawValue;
    }
  }
}
