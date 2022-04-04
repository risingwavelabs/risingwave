package com.risingwave.common.config;

import static java.util.Objects.requireNonNull;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.util.Properties;
import javax.annotation.Nullable;

/** Config entry represents a immutable config item. */
public class ConfigEntry<T> {
  // TODO: make config entry mutable and refactor [`Configurations.class`] API.
  private final String key;
  private final String doc;
  private final T defaultValue;
  private final boolean optional;
  private final Validator<T> validator;
  private final Parser<T> parser;

  private ConfigEntry(Builder<T> builder) {
    this.key = requireNonNull(builder.key, "Key can't be null!");
    this.doc = requireNonNull(builder.doc, "Doc can't be null!");
    this.defaultValue = builder.defaultValue;
    this.optional = builder.optional;
    this.validator = requireNonNull(builder.validator, "Validator can't be null!");
    this.parser = requireNonNull(builder.parser, "Converter can't be null!");
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

  public Parser<?> getParser() {
    return parser;
  }

  @Nullable
  public T getValue(Properties config) {
    String value = config.getProperty(key);
    if (value != null) {
      T configValue = parser.convert(value);
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

  /**
   * To build a config entry, using the config entry builder with default value and
   * parser/validator.
   */
  public static class Builder<T> {
    private final String key;
    private String doc = "";
    private T defaultValue = null;
    private boolean optional = true;
    private Validator<T> validator = new EmptyValidator<T>();
    private Parser<T> parser;

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

    public Builder<T> withConverter(Parser<T> parser) {
      this.parser = parser;
      return this;
    }

    public ConfigEntry<T> build() {
      return new ConfigEntry<>(this);
    }
  }
}
