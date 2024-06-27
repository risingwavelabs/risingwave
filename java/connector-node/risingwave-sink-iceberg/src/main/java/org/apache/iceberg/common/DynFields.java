/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.common;

import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

public class DynFields {

  private DynFields() {}

  /**
   * Convenience wrapper class around {@link Field}.
   *
   * <p>Allows callers to invoke the wrapped method with all Exceptions wrapped by RuntimeException,
   * or with a single Exception catch block.
   */
  public static class UnboundField<T> {
    private final Field field;
    private final String name;

    private UnboundField(Field field, String name) {
      this.field = field;
      this.name = name;
    }

    @SuppressWarnings("unchecked")
    public T get(Object target) {
      try {
        return (T) field.get(target);
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }

    public void set(Object target, T value) {
      try {
        field.set(target, value);
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("class", field.getDeclaringClass().toString())
          .add("name", name)
          .add("type", field.getType())
          .toString();
    }

    /**
     * Returns this method as a BoundMethod for the given receiver.
     *
     * @param target an Object on which to get or set this field
     * @return a {@link BoundField} for this field and the target
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     */
    public BoundField<T> bind(Object target) {
      Preconditions.checkState(
          !isStatic() || this == AlwaysNull.INSTANCE, "Cannot bind static field %s", name);
      Preconditions.checkArgument(
          field.getDeclaringClass().isAssignableFrom(target.getClass()),
          "Cannot bind field %s to instance of %s",
          name,
          target.getClass());

      return new BoundField<>(this, target);
    }

    /**
     * Returns this field as a StaticField.
     *
     * @return a {@link StaticField} for this field
     * @throws IllegalStateException if the method is not static
     */
    public StaticField<T> asStatic() {
      Preconditions.checkState(isStatic(), "Field %s is not static", name);
      return new StaticField<>(this);
    }

    /** Returns whether the field is a static field. */
    public boolean isStatic() {
      return Modifier.isStatic(field.getModifiers());
    }

    /** Returns whether the field is always null. */
    public boolean isAlwaysNull() {
      return this == AlwaysNull.INSTANCE;
    }
  }

  private static class AlwaysNull extends UnboundField<Void> {
    private static final AlwaysNull INSTANCE = new AlwaysNull();

    private AlwaysNull() {
      super(null, "AlwaysNull");
    }

    @Override
    public Void get(Object target) {
      return null;
    }

    @Override
    public void set(Object target, Void value) {}

    @Override
    public String toString() {
      return "Field(AlwaysNull)";
    }

    @Override
    public boolean isStatic() {
      return true;
    }

    @Override
    public boolean isAlwaysNull() {
      return true;
    }
  }

  public static class StaticField<T> {
    private final UnboundField<T> field;

    private StaticField(UnboundField<T> field) {
      this.field = field;
    }

    public T get() {
      return field.get(null);
    }

    public void set(T value) {
      field.set(null, value);
    }
  }

  public static class BoundField<T> {
    private final UnboundField<T> field;
    private final Object target;

    private BoundField(UnboundField<T> field, Object target) {
      this.field = field;
      this.target = target;
    }

    public T get() {
      return field.get(target);
    }

    public void set(T value) {
      field.set(target, value);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private UnboundField<?> field = null;
    private final Set<String> candidates = Sets.newHashSet();
    private boolean defaultAlwaysNull = false;

    private Builder() {}

    /**
     * Set the {@link ClassLoader} used to lookup classes by name.
     *
     * <p>If not set, the current thread's ClassLoader is used.
     *
     * @param newLoader a ClassLoader
     * @return this Builder for method chaining
     */
    public Builder loader(ClassLoader newLoader) {
      return this;
    }

    /**
     * Instructs this builder to return AlwaysNull if no implementation is found.
     *
     * @return this Builder for method chaining
     */
    public Builder defaultAlwaysNull() {
      this.defaultAlwaysNull = true;
      return this;
    }

    /**
     * Checks for an implementation, first finding the class by name.
     *
     * @param className name of a class
     * @param fieldName name of the field
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getField(String)
     */
    public Builder impl(String className, String fieldName) {
      // don't do any work if an implementation has been found
      if (field != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className);
        impl(targetClass, fieldName);
      } catch (ClassNotFoundException e) {
        // not the right implementation
        candidates.add(className + "." + fieldName);
      }
      return this;
    }

    /**
     * Checks for an implementation.
     *
     * @param targetClass a class instance
     * @param fieldName name of a field (different from constructor)
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getField(String)
     */
    public Builder impl(Class<?> targetClass, String fieldName) {
      // don't do any work if an implementation has been found
      if (field != null || targetClass == null) {
        return this;
      }

      try {
        this.field = new UnboundField<>(targetClass.getField(fieldName), fieldName);
      } catch (NoSuchFieldException e) {
        // not the right implementation
        candidates.add(targetClass.getName() + "." + fieldName);
      }
      return this;
    }

    /**
     * Checks for a hidden implementation, first finding the class by name.
     *
     * @param className name of a class
     * @param fieldName name of a field (different from constructor)
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getField(String)
     */
    public Builder hiddenImpl(String className, String fieldName) {
      // don't do any work if an implementation has been found
      if (field != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className);
        hiddenImpl(targetClass, fieldName);
      } catch (ClassNotFoundException e) {
        // not the right implementation
        candidates.add(className + "." + fieldName);
      }
      return this;
    }

    /**
     * Checks for a hidden implementation.
     *
     * @param targetClass a class instance
     * @param fieldName name of a field (different from constructor)
     * @return this Builder for method chaining
     * @see Class#forName(String)
     * @see Class#getField(String)
     */
    public Builder hiddenImpl(Class<?> targetClass, String fieldName) {
      // don't do any work if an implementation has been found
      if (field != null || targetClass == null) {
        return this;
      }

      try {
        Field hidden = targetClass.getDeclaredField(fieldName);
        AccessController.doPrivileged(new MakeFieldAccessible(hidden));
        this.field = new UnboundField(hidden, fieldName);
      } catch (SecurityException | NoSuchFieldException e) {
        // unusable
        candidates.add(targetClass.getName() + "." + fieldName);
      }
      return this;
    }

    /**
     * Returns the first valid implementation as a UnboundField or throws a NoSuchFieldException if
     * there is none.
     *
     * @param <T> Java class stored in the field
     * @return a {@link UnboundField} with a valid implementation
     * @throws NoSuchFieldException if no implementation was found
     */
    @SuppressWarnings("unchecked")
    public <T> UnboundField<T> buildChecked() throws NoSuchFieldException {
      if (field != null) {
        return (UnboundField<T>) field;
      } else if (defaultAlwaysNull) {
        return (UnboundField<T>) AlwaysNull.INSTANCE;
      } else {
        throw new NoSuchFieldException(
            "Cannot find field from candidates: " + Joiner.on(", ").join(candidates));
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a NoSuchMethodException if
     * there is none.
     *
     * @param target an Object on which to get and set the field
     * @param <T> Java class stored in the field
     * @return a {@link BoundField} with a valid implementation and target
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws NoSuchFieldException if no implementation was found
     */
    public <T> BoundField<T> buildChecked(Object target) throws NoSuchFieldException {
      return this.<T>buildChecked().bind(target);
    }

    /**
     * Returns the first valid implementation as a UnboundField or throws a NoSuchFieldException if
     * there is none.
     *
     * @param <T> Java class stored in the field
     * @return a {@link UnboundField} with a valid implementation
     * @throws RuntimeException if no implementation was found
     */
    @SuppressWarnings("unchecked")
    public <T> UnboundField<T> build() {
      if (field != null) {
        return (UnboundField<T>) field;
      } else if (defaultAlwaysNull) {
        return (UnboundField<T>) AlwaysNull.INSTANCE;
      } else {
        throw new RuntimeException(
            "Cannot find field from candidates: " + Joiner.on(", ").join(candidates));
      }
    }

    /**
     * Returns the first valid implementation as a BoundMethod or throws a RuntimeException if there
     * is none.
     *
     * @param target an Object on which to get and set the field
     * @param <T> Java class stored in the field
     * @return a {@link BoundField} with a valid implementation and target
     * @throws IllegalStateException if the method is static
     * @throws IllegalArgumentException if the receiver's class is incompatible
     * @throws RuntimeException if no implementation was found
     */
    public <T> BoundField<T> build(Object target) {
      return this.<T>build().bind(target);
    }

    /**
     * Returns the first valid implementation as a StaticField or throws a NoSuchFieldException if
     * there is none.
     *
     * @param <T> Java class stored in the field
     * @return a {@link StaticField} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws NoSuchFieldException if no implementation was found
     */
    public <T> StaticField<T> buildStaticChecked() throws NoSuchFieldException {
      return this.<T>buildChecked().asStatic();
    }

    /**
     * Returns the first valid implementation as a StaticField or throws a RuntimeException if there
     * is none.
     *
     * @param <T> Java class stored in the field
     * @return a {@link StaticField} with a valid implementation
     * @throws IllegalStateException if the method is not static
     * @throws RuntimeException if no implementation was found
     */
    public <T> StaticField<T> buildStatic() {
      return this.<T>build().asStatic();
    }
  }

  private static class MakeFieldAccessible implements PrivilegedAction<Void> {
    private Field hidden;

    MakeFieldAccessible(Field hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }
}
