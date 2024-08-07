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
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.connector.BaseSchema;
import lombok.ToString;

/** Represents a Jdbc Schema (Database) entity in the Jdbc schema. */
@ToString
public class JdbcSchema extends BaseSchema {

  private JdbcSchema() {}

  public static class Builder extends BaseSchemaBuilder<Builder, JdbcSchema> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected JdbcSchema internalBuild() {
      JdbcSchema jdbcSchema = new JdbcSchema();
      jdbcSchema.name = name;
      jdbcSchema.comment = comment;
      jdbcSchema.properties = properties;
      jdbcSchema.auditInfo = auditInfo;
      return jdbcSchema;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
