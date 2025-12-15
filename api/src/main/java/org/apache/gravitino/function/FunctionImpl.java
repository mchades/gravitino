/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.annotation.Evolving;

/**
 * Base class of function implementations.
 *
 * <p>A function implementation must declare its language and optional external resources. Concrete
 * implementations are provided by {@link SQLImpl}, {@link JavaImpl}, and {@link PythonImpl}.
 */
@Evolving
public abstract class FunctionImpl {
  /** Supported implementation languages. */
  public enum Language {
    SQL,
    JAVA,
    PYTHON
  }

  private final Language language;
  private final FunctionResources resources;
  private final Map<String, String> properties;

  FunctionImpl(Language language, FunctionResources resources, Map<String, String> properties) {
    Preconditions.checkNotNull(language, "Function implementation language must be set");
    this.language = language;
    this.resources = resources == null ? FunctionResources.empty() : resources;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
  }

  /** Create a SQL implementation. */
  public static SQLImpl ofSql(String dialect, String sql) {
    return ofSql(dialect, sql, null, null);
  }

  /** Create a SQL implementation. */
  public static SQLImpl ofSql(
      String dialect, String sql, FunctionResources resources, Map<String, String> properties) {
    return new SQLImpl(dialect, sql, resources, properties);
  }

  /** Create a Java implementation. */
  public static JavaImpl ofJava(String className) {
    return ofJava(className, null, null);
  }

  /** Create a Java implementation. */
  public static JavaImpl ofJava(
      String className, FunctionResources resources, Map<String, String> properties) {
    return new JavaImpl(className, resources, properties);
  }

  /** Create a Python implementation. */
  public static PythonImpl ofPython(String handler) {
    return ofPython(handler, null, null, null);
  }

  /** Create a Python implementation. */
  public static PythonImpl ofPython(
      String handler,
      String codeBlock,
      FunctionResources resources,
      Map<String, String> properties) {
    return new PythonImpl(handler, codeBlock, resources, properties);
  }

  /**
   * @return The implementation language.
   */
  public Language language() {
    return language;
  }

  /**
   * @return The external resources required by this implementation.
   */
  public FunctionResources resources() {
    return resources;
  }

  /**
   * @return The additional properties of this implementation.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
