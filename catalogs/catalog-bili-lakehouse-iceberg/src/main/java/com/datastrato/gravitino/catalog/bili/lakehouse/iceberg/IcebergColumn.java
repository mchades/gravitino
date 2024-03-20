/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.bili.lakehouse.iceberg;

import com.datastrato.gravitino.connector.BaseColumn;
import lombok.EqualsAndHashCode;

/** Represents a column in the Iceberg column. */
@EqualsAndHashCode(callSuper = true)
public class IcebergColumn extends BaseColumn {

  private IcebergColumn() {}

  /** A builder class for constructing IcebergColumn instances. */
  public static class Builder extends BaseColumnBuilder<Builder, IcebergColumn> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}
    /**
     * Internal method to build a IcebergColumn instance using the provided values.
     *
     * @return A new IcebergColumn instance with the configured values.
     */
    @Override
    protected IcebergColumn internalBuild() {
      IcebergColumn icebergColumn = new IcebergColumn();
      icebergColumn.name = name;
      icebergColumn.comment = comment;
      icebergColumn.dataType = dataType;
      icebergColumn.nullable = nullable;
      icebergColumn.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
      return icebergColumn;
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
