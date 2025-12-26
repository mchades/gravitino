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
package org.apache.gravitino.meta;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;

/** A class representing a function entity in Apache Gravitino. */
@ToString
public class FunctionEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The function's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The function's name");
  public static final Field VERSION =
      Field.required("version", Integer.class, "The function's version");
  public static final Field FUNCTION_TYPE =
      Field.required("functionType", FunctionType.class, "The function's type");
  public static final Field DETERMINISTIC =
      Field.required("deterministic", Boolean.class, "Whether the function is deterministic");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The function's comment");
  public static final Field DEFINITIONS =
      Field.required("definitions", FunctionDefinition[].class, "The function's definitions");
  public static final Field RETURN_TYPE =
      Field.optional("returnType", Type.class, "The function's return type");
  public static final Field RETURN_COLUMNS =
      Field.optional("returnColumns", FunctionColumn[].class, "The function's return columns");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the function");

  private Long id;
  private String name;
  private Integer version;
  private Namespace namespace;
  private AuditInfo auditInfo;

  @Getter
  @Accessors(fluent = true)
  private FunctionType functionType;

  @Getter
  @Accessors(fluent = true)
  private Boolean deterministic;

  @Getter
  @Accessors(fluent = true)
  private String comment;

  @Getter
  @Accessors(fluent = true)
  private FunctionDefinition[] definitions;

  @Getter
  @Accessors(fluent = true)
  private Type returnType;

  @Getter
  @Accessors(fluent = true)
  private FunctionColumn[] returnColumns;

  private FunctionEntity() {}

  /**
   * Returns a map of fields and their corresponding values for this function entity.
   *
   * @return An unmodifiable map of Field to Object representing the entity's schema with values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(VERSION, version);
    fields.put(FUNCTION_TYPE, functionType);
    fields.put(DETERMINISTIC, deterministic);
    fields.put(COMMENT, comment);
    fields.put(DEFINITIONS, definitions);
    fields.put(RETURN_TYPE, returnType);
    fields.put(RETURN_COLUMNS, returnColumns);
    fields.put(AUDIT_INFO, auditInfo);
    return Collections.unmodifiableMap(fields);
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.FUNCTION;
  }

  /**
   * Returns the unique identifier of this function entity.
   *
   * @return The unique identifier of the function entity.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns the name of the function.
   *
   * @return The name of the function.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the namespace of the function.
   *
   * @return The namespace of the function.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the audit information of the function.
   *
   * @return The audit information of the function.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the version of the function.
   *
   * @return The version of the function.
   */
  public Integer version() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FunctionEntity)) return false;
    FunctionEntity that = (FunctionEntity) o;
    return Objects.equal(id, that.id)
        && Objects.equal(name, that.name)
        && Objects.equal(namespace, that.namespace)
        && Objects.equal(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, namespace, version);
  }

  /** Builder class for creating FunctionEntity instances. */
  public static class Builder {
    private final FunctionEntity functionEntity;

    private Builder() {
      this.functionEntity = new FunctionEntity();
    }

    /**
     * Sets the unique identifier for the function entity.
     *
     * @param id The unique identifier.
     * @return This builder instance.
     */
    public Builder withId(Long id) {
      functionEntity.id = id;
      return this;
    }

    /**
     * Sets the name of the function.
     *
     * @param name The name of the function.
     * @return This builder instance.
     */
    public Builder withName(String name) {
      functionEntity.name = name;
      return this;
    }

    /**
     * Sets the namespace of the function.
     *
     * @param namespace The namespace of the function.
     * @return This builder instance.
     */
    public Builder withNamespace(Namespace namespace) {
      functionEntity.namespace = namespace;
      return this;
    }

    /**
     * Sets the version of the function.
     *
     * @param version The version of the function.
     * @return This builder instance.
     */
    public Builder withVersion(Integer version) {
      functionEntity.version = version;
      return this;
    }

    /**
     * Sets the function type.
     *
     * @param functionType The function type.
     * @return This builder instance.
     */
    public Builder withFunctionType(FunctionType functionType) {
      functionEntity.functionType = functionType;
      return this;
    }

    /**
     * Sets whether the function is deterministic.
     *
     * @param deterministic Whether the function is deterministic.
     * @return This builder instance.
     */
    public Builder withDeterministic(Boolean deterministic) {
      functionEntity.deterministic = deterministic;
      return this;
    }

    /**
     * Sets the comment for the function.
     *
     * @param comment The comment.
     * @return This builder instance.
     */
    public Builder withComment(String comment) {
      functionEntity.comment = comment;
      return this;
    }

    /**
     * Sets the function definitions.
     *
     * @param definitions The function definitions.
     * @return This builder instance.
     */
    public Builder withDefinitions(FunctionDefinition[] definitions) {
      functionEntity.definitions = definitions;
      return this;
    }

    /**
     * Sets the return type for scalar/aggregate functions.
     *
     * @param returnType The return type.
     * @return This builder instance.
     */
    public Builder withReturnType(Type returnType) {
      functionEntity.returnType = returnType;
      return this;
    }

    /**
     * Sets the return columns for table functions.
     *
     * @param returnColumns The return columns.
     * @return This builder instance.
     */
    public Builder withReturnColumns(FunctionColumn[] returnColumns) {
      functionEntity.returnColumns = returnColumns;
      return this;
    }

    /**
     * Sets the audit information for the function.
     *
     * @param auditInfo The audit information.
     * @return This builder instance.
     */
    public Builder withAuditInfo(AuditInfo auditInfo) {
      functionEntity.auditInfo = auditInfo;
      return this;
    }

    /**
     * Builds the FunctionEntity instance.
     *
     * @return The FunctionEntity instance.
     */
    public FunctionEntity build() {
      functionEntity.validate();
      return functionEntity;
    }
  }

  /**
   * Creates a new builder for constructing FunctionEntity instances.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
