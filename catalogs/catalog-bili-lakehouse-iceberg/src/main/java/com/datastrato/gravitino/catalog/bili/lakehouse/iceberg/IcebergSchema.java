/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.bili.lakehouse.iceberg;

import com.datastrato.gravitino.catalog.rel.BaseSchema;
import lombok.ToString;

/** Represents an Iceberg Schema (Database) entity in the Iceberg schema. */
@ToString
public class IcebergSchema extends BaseSchema {

  private IcebergSchema() {}

  /** A builder class for constructing IcebergSchema instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, IcebergSchema> {

    @Override
    protected IcebergSchema internalBuild() {
      IcebergSchema icebergSchema = new IcebergSchema();
      icebergSchema.name = name;
      icebergSchema.comment =
          null == comment
              ? (null == properties
                  ? null
                  : properties.get(IcebergSchemaPropertiesMetadata.COMMENT))
              : comment;
      icebergSchema.properties = properties;
      icebergSchema.auditInfo = auditInfo;
      return icebergSchema;
    }
  }
}
