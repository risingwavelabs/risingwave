package com.risingwave.common.config;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.risingwave.common.exception.PgErrorCode.INTERNAL_ERROR;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

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

public class Configuration {
  private final Map<String, Object> confData = new HashMap<>();

  @SuppressWarnings({"unchecked"})
  public <T> T get(ConfigEntry<T> key) {
    return (T) getRawValue(key);
  }

  public <T> void set(ConfigEntry<T> key, T value) {
    checkNotNull(value, "Value can't be null!");
    setRawValue(key, value);
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

  public static Configuration load(String filename, Class<?> klass) {
    try (FileInputStream fin = new FileInputStream(filename)) {
      return load(fin, klass);
    } catch (Exception e) {
      throw new PgException(INTERNAL_ERROR, e);
    }
  }

  public static Configuration load(InputStream in, Class<?> klass) {
    try {
      Properties props = new Properties();
      props.load(in);

      Configuration configuration = new Configuration();
      List<ConfigEntry<?>> configEntries = loadConfigEntries(klass);
      configEntries.forEach(entry -> configuration.setRawValue(entry, entry.getValue(props)));

      return configuration;
    } catch (IOException e) {
      throw new PgException(INTERNAL_ERROR, e);
    }
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
