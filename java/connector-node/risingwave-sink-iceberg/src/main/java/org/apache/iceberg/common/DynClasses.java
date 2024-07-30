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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

import java.util.Set;

public class DynClasses {

  private DynClasses() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Class<?> foundClass = null;
    private boolean nullOk = false;
    private Set<String> classNames = Sets.newLinkedHashSet();

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
     * Checks for an implementation of the class by name.
     *
     * @param className name of a class
     * @return this Builder for method chaining
     */
    public Builder impl(String className) {
      classNames.add(className);

      if (foundClass != null) {
        return this;
      }

      try {
        this.foundClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        // not the right implementation
      }

      return this;
    }

    /**
     * Instructs this builder to return null if no class is found, rather than throwing an
     * Exception.
     *
     * @return this Builder for method chaining
     */
    public Builder orNull() {
      this.nullOk = true;
      return this;
    }

    /**
     * Returns the first implementation or throws ClassNotFoundException if one was not found.
     *
     * @param <S> Java superclass
     * @return a {@link Class} for the first implementation found
     * @throws ClassNotFoundException if no implementation was found
     */
    @SuppressWarnings("unchecked")
    public <S> Class<? extends S> buildChecked() throws ClassNotFoundException {
      if (!nullOk && foundClass == null) {
        throw new ClassNotFoundException(
            "Cannot find class; alternatives: " + Joiner.on(", ").join(classNames));
      }
      return (Class<? extends S>) foundClass;
    }

    /**
     * Returns the first implementation or throws RuntimeException if one was not found.
     *
     * @param <S> Java superclass
     * @return a {@link Class} for the first implementation found
     * @throws RuntimeException if no implementation was found
     */
    @SuppressWarnings("unchecked")
    public <S> Class<? extends S> build() {
      if (!nullOk && foundClass == null) {
        throw new RuntimeException(
            "Cannot find class; alternatives: " + Joiner.on(", ").join(classNames));
      }
      return (Class<? extends S>) foundClass;
    }
  }
}
