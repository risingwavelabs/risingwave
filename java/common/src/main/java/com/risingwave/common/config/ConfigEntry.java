package com.risingwave.common.config;

import static java.util.Objects.requireNonNull;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.util.Properties;
import javax.annotation.Nullable;

public class ConfigEntry<T> {
  private final String key;
  private final String doc;
  private final T defaultValue;
  private final boolean optional;
  private final Validator<T> validator;
  private final Converter<T> converter;

  private ConfigEntry(Builder<T> builder) {
    this.key = requireNonNull(builder.key, "Key can't be null!");
    this.doc = requireNonNull(builder.doc, "Doc can't be null!");
    this.defaultValue = builder.defaultValue;
    this.optional = builder.optional;
    this.validator = requireNonNull(builder.validator, "Validator can't be null!");
    this.converter = requireNonNull(builder.converter, "Converter can't be null!");
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  public boolean isOptional() {
    return optional;
  }

  public String getKey() {
    return key;
  }

  public static <T> Builder<T> builder(String key) {
    return new Builder<>(key);
  }

  @Nullable
  public T getValue(Properties config) {
    String value = config.getProperty(key);
    if (value != null) {
      T configValue = converter.convert(value);
      validator.validate(configValue);
      return configValue;
    } else {
      if (defaultValue != null) {
        return defaultValue;
      }

      if (!optional) {
        throw new PgException(
            PgErrorCode.CONFIG_FILE_ERROR, "Config %s is missing and has no default value!", key);
      }

      return null;
    }
  }

  public static class Builder<T> {
    private final String key;
    private String doc = "";
    private T defaultValue = null;
    private boolean optional = true;
    private Validator<T> validator = new EmptyValidator<T>();
    private Converter<T> converter;

    public Builder(String key) {
      this.key = key;
    }

    public Builder<T> withDoc(String doc) {
      this.doc = doc;
      return this;
    }

    public Builder<T> withDefaultValue(T t) {
      this.defaultValue = t;
      return this;
    }

    public Builder<T> setOptional(boolean optional) {
      this.optional = optional;
      return this;
    }

    public Builder<T> withValidator(Validator<T> validator) {
      this.validator = validator;
      return this;
    }

    public Builder<T> withConverter(Converter<T> converter) {
      this.converter = converter;
      return this;
    }

    public ConfigEntry<T> build() {
      return new ConfigEntry<>(this);
    }
  }
}
