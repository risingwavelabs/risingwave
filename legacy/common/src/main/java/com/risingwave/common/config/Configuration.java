package com.risingwave.common.config;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.risingwave.common.exception.PgErrorCode.INTERNAL_ERROR;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Manage the configurations of all registered parameters. To add a parameters. For example,
 * `enable_mergejoin`. Create a ConfigEntry in BatchPlannerConfigurations (with key =
 * "enable_mergejoin"). Set up default value and corresponding parser/converter. Then 'SET
 * enable_mergejoin to false` should take effects in session level.
 */
public class Configuration {

  /**
   * string key -> config value (T). Note that this config map is designed to store server-level
   * parameters. Different from the map in `SessionConfiguration`.
   */
  private final Map<String, Object> confData = new HashMap<>();

  /**
   * string key -> config entry. Server level config map. When each session can not find
   * corresponding config, get the default value from registry. `SET` can not change the value here.
   */
  private static final ImmutableMap<String, ConfigEntry<?>> confRegistry = loadConfigRegistry();

  @SuppressWarnings({"unchecked"})
  public <T> T get(ConfigEntry<T> key) {
    return (T) getRawValue(key);
  }

  public <T> void set(ConfigEntry<T> key, T value) {
    checkNotNull(value, "Value can't be null!");
    setRawValue(key, value);
  }

  public ConfigEntry<?> getConfigEntry(String stringKey) {
    var configEntry = confRegistry.get(stringKey);
    if (configEntry == null) {
      throw new PgException(
          PgErrorCode.CONFIG_FILE_ERROR, "Config entry '%s' not found!", stringKey);
    } else {
      return configEntry;
    }
  }

  @Override
  public String toString() {
    var helper = MoreObjects.toStringHelper("Configuration");
    for (var entry : confData.entrySet()) {
      helper.add(entry.getKey(), entry.getValue());
    }
    return helper.toString();
  }

  private Object getRawValue(ConfigEntry<?> entry) {
    Object rawValue = confData.getOrDefault(entry.getKey(), entry.getDefaultValue());
    if (rawValue == null) {
      if (!entry.isOptional()) {
        throw new PgException(
            PgErrorCode.CONFIG_FILE_ERROR,
            "Config %s is missing and has no default value!",
            entry.getKey());
      }

      return null;
    } else {
      return rawValue;
    }
  }

  private void setRawValue(ConfigEntry<?> key, Object value) {
    checkNotNull(value, "Value can't ben null!");
    confData.put(key.getKey(), value);
  }

  public static Configuration load(String filename, Class<?>... classes) {
    try (FileInputStream fin = new FileInputStream(filename)) {
      return load(fin, classes);
    } catch (Exception e) {
      throw new PgException(INTERNAL_ERROR, e);
    }
  }

  // Integrate configuration classes and load the corresponding values.
  public static Configuration load(InputStream in, Class<?>... classes) {
    try {
      Properties props = new Properties();
      props.load(in);

      Configuration configuration = new Configuration();
      for (var klass : classes) {
        List<ConfigEntry<?>> configEntries = loadConfigEntries(klass);
        configEntries.forEach(entry -> configuration.setRawValue(entry, entry.getValue(props)));
      }

      return configuration;
    } catch (IOException e) {
      throw new PgException(INTERNAL_ERROR, e);
    }
  }

  private static ImmutableMap<String, ConfigEntry<?>> loadConfigRegistry() {
    // Load batch planner options.
    List<List<ConfigEntry<?>>> configEntriesList =
        List.of(
            loadConfigEntries(BatchPlannerConfigurations.class),
            loadConfigEntries(StreamPlannerConfigurations.class));
    var mapBuilder = new ImmutableMap.Builder<String, ConfigEntry<?>>();
    for (var configEntries : configEntriesList) {
      configEntries.forEach(
          entry -> {
            mapBuilder.put(entry.getKey(), entry);
          });
    }
    return mapBuilder.build();
  }

  private static List<ConfigEntry<?>> loadConfigEntries(Class<?> klass) {
    return Arrays.stream(klass.getDeclaredFields())
        .filter(f -> f.getAnnotation(Config.class) != null)
        .filter(f -> f.getType() == ConfigEntry.class)
        .filter(
            f ->
                isPublic(f.getModifiers())
                    && isStatic(f.getModifiers())
                    && isFinal(f.getModifiers()))
        .map(
            f -> {
              try {
                return (ConfigEntry<?>) f.get(null);
              } catch (IllegalAccessException e) {
                throw new PgException(INTERNAL_ERROR, e);
              }
            })
        .collect(Collectors.toList());
  }
}
