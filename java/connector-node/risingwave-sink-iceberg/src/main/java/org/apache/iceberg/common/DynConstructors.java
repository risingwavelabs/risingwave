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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Throwables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;

/** Copied from parquet-common */
public class DynConstructors {

  private DynConstructors() {}

  public static class Ctor<C> extends DynMethods.UnboundMethod {
    private final Constructor<C> ctor;
    private final Class<? extends C> constructed;

    private Ctor(Constructor<C> constructor, Class<? extends C> constructed) {
      super(null, "newInstance");
      this.ctor = constructor;
      this.constructed = constructed;
    }

    public Class<? extends C> getConstructedClass() {
      return constructed;
    }

    public C newInstanceChecked(Object... args) throws Exception {
      try {
        if (args.length > ctor.getParameterCount()) {
          return ctor.newInstance(Arrays.copyOfRange(args, 0, ctor.getParameterCount()));
        } else {
          return ctor.newInstance(args);
        }
      } catch (InstantiationException | IllegalAccessException e) {
        throw e;
      } catch (InvocationTargetException e) {
        Throwables.propagateIfInstanceOf(e.getCause(), Exception.class);
        Throwables.propagateIfInstanceOf(e.getCause(), RuntimeException.class);
        throw Throwables.propagate(e.getCause());
      }
    }

    public C newInstance(Object... args) {
      try {
        return newInstanceChecked(args);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, RuntimeException.class);
        throw Throwables.propagate(e);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R invoke(Object target, Object... args) {
      Preconditions.checkArgument(
          target == null, "Invalid call to constructor: target must be null");
      return (R) newInstance(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R invokeChecked(Object target, Object... args) throws Exception {
      Preconditions.checkArgument(
          target == null, "Invalid call to constructor: target must be null");
      return (R) newInstanceChecked(args);
    }

    @Override
    public DynMethods.BoundMethod bind(Object receiver) {
      throw new IllegalStateException("Cannot bind constructors");
    }

    @Override
    public boolean isStatic() {
      return true;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(constructor=" + ctor + ", class=" + constructed + ")";
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Class<?> baseClass) {
    return new Builder(baseClass);
  }

  public static class Builder {
    private final Class<?> baseClass;
    private Ctor ctor = null;
    private Map<String, Throwable> problems = Maps.newHashMap();

    public Builder(Class<?> baseClass) {
      this.baseClass = baseClass;
    }

    public Builder() {
      this.baseClass = null;
    }

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

    public Builder impl(String className, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Class<?> targetClass = Class.forName(className);
        impl(targetClass, types);
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        // cannot load this implementation
        problems.put(className, e);
      }
      return this;
    }

    public <T> Builder impl(Class<T> targetClass, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        ctor = new Ctor<T>(targetClass.getConstructor(types), targetClass);
      } catch (NoSuchMethodException e) {
        // not the right implementation
        problems.put(methodName(targetClass, types), e);
      }
      return this;
    }

    public Builder hiddenImpl(Class<?>... types) {
      hiddenImpl(baseClass, types);
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder hiddenImpl(String className, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Class targetClass = Class.forName(className);
        hiddenImpl(targetClass, types);
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        // cannot load this implementation
        problems.put(className, e);
      }
      return this;
    }

    public <T> Builder hiddenImpl(Class<T> targetClass, Class<?>... types) {
      // don't do any work if an implementation has been found
      if (ctor != null) {
        return this;
      }

      try {
        Constructor<T> hidden = targetClass.getDeclaredConstructor(types);
        AccessController.doPrivileged(new MakeAccessible(hidden));
        ctor = new Ctor<T>(hidden, targetClass);
      } catch (SecurityException e) {
        // unusable
        problems.put(methodName(targetClass, types), e);
      } catch (NoSuchMethodException e) {
        // not the right implementation
        problems.put(methodName(targetClass, types), e);
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    public <C> Ctor<C> buildChecked() throws NoSuchMethodException {
      if (ctor != null) {
        return ctor;
      }
      throw buildCheckedException(baseClass, problems);
    }

    @SuppressWarnings("unchecked")
    public <C> Ctor<C> build() {
      if (ctor != null) {
        return ctor;
      }
      throw buildRuntimeException(baseClass, problems);
    }
  }

  private static class MakeAccessible implements PrivilegedAction<Void> {
    private Constructor<?> hidden;

    MakeAccessible(Constructor<?> hidden) {
      this.hidden = hidden;
    }

    @Override
    public Void run() {
      hidden.setAccessible(true);
      return null;
    }
  }

  private static NoSuchMethodException buildCheckedException(
      Class<?> baseClass, Map<String, Throwable> problems) {
    NoSuchMethodException exc =
        new NoSuchMethodException(
            "Cannot find constructor for " + baseClass + "\n" + formatProblems(problems));
    problems.values().forEach(exc::addSuppressed);
    return exc;
  }

  private static RuntimeException buildRuntimeException(
      Class<?> baseClass, Map<String, Throwable> problems) {
    RuntimeException exc =
        new RuntimeException(
            "Cannot find constructor for " + baseClass + "\n" + formatProblems(problems));
    problems.values().forEach(exc::addSuppressed);
    return exc;
  }

  private static String formatProblems(Map<String, Throwable> problems) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, Throwable> problem : problems.entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append("\n");
      }
      sb.append("\tMissing ")
          .append(problem.getKey())
          .append(" [")
          .append(problem.getValue().getClass().getName())
          .append(": ")
          .append(problem.getValue().getMessage())
          .append("]");
    }
    return sb.toString();
  }

  private static String methodName(Class<?> targetClass, Class<?>... types) {
    StringBuilder sb = new StringBuilder();
    sb.append(targetClass.getName()).append("(");
    boolean first = true;
    for (Class<?> type : types) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(type.getName());
    }
    sb.append(")");
    return sb.toString();
  }
}
