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
import java.util.Map;
import java.util.Objects;

/** SQL implementation with dialect and SQL body. */
public class SQLImpl extends FunctionImpl {
  private final String dialect;
  private final String sql;

  SQLImpl(String dialect, String sql, FunctionResources resources, Map<String, String> properties) {
    super(Language.SQL, resources, properties);
    this.dialect = Preconditions.checkNotNull(dialect, "SQL dialect cannot be null");
    this.sql = Preconditions.checkNotNull(sql, "SQL text cannot be null");
  }

  /**
   * @return The SQL dialect.
   */
  public String dialect() {
    return dialect;
  }

  /**
   * @return The SQL body.
   */
  public String sql() {
    return sql;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SQLImpl)) {
      return false;
    }
    SQLImpl that = (SQLImpl) obj;
    return Objects.equals(language(), that.language())
        && Objects.equals(resources(), that.resources())
        && Objects.equals(properties(), that.properties())
        && Objects.equals(dialect, that.dialect)
        && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(language(), resources(), properties(), dialect, sql);
  }

  @Override
  public String toString() {
    return "SQLImpl{"
        + "language='"
        + language()
        + '\''
        + ", dialect='"
        + dialect
        + '\''
        + ", sql='"
        + sql
        + '\''
        + ", resources="
        + resources()
        + ", properties="
        + properties()
        + '}';
  }
}
