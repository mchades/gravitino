/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.bili.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.bili.lakehouse.iceberg.IcebergTablePropertiesMetadata.DISTRIBUTION_MODE;

import com.datastrato.gravitino.catalog.TableOperations;
import com.datastrato.gravitino.catalog.bili.lakehouse.iceberg.converter.ConvertUtil;
import com.datastrato.gravitino.catalog.bili.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import com.datastrato.gravitino.catalog.rel.BaseTable;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;

/** Represents an Iceberg Table entity in the Iceberg table. */
@ToString
@Getter
public class IcebergTable extends BaseTable {

  /**
   * A reserved property to specify the location of the table. The files of the table should be
   * under this location.
   */
  public static final String PROP_LOCATION = "location";

  /** A reserved property to specify the provider of the table. */
  public static final String PROP_PROVIDER = "provider";

  /** The default provider of the table. */
  public static final String DEFAULT_ICEBERG_PROVIDER = "iceberg";

  public static final String ICEBERG_COMMENT_FIELD_NAME = "comment";

  private String location;

  private IcebergTable() {}

  /**
   * Creates a new IcebergTable instance from a Table and a Builder.
   *
   * @param table The inner Table representing the IcebergTable.
   * @param tableName The name of Table.
   * @return A new IcebergTable instance.
   */
  public static IcebergTable fromIcebergTable(TableMetadata table, String tableName) {
    Map<String, String> properties = table.properties();
    Schema schema = table.schema();
    Transform[] partitionSpec = FromIcebergPartitionSpec.fromPartitionSpec(table.spec(), schema);
    Distribution distribution = Distributions.NONE;
    String distributionName = properties.get(DISTRIBUTION_MODE);
    if (null != distributionName) {
      switch (DistributionMode.fromName(distributionName)) {
        case HASH:
          distribution = Distributions.HASH;
          break;
        case RANGE:
          distribution = Distributions.RANGE;
          break;
        default:
          // do nothing
          break;
      }
    }
    IcebergColumn[] icebergColumns =
        schema.columns().stream().map(ConvertUtil::fromNestedField).toArray(IcebergColumn[]::new);
    return new Builder()
        .withComment(table.property(IcebergTablePropertiesMetadata.COMMENT, null))
        .withLocation(table.location())
        .withProperties(properties)
        .withColumns(icebergColumns)
        .withName(tableName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withPartitioning(partitionSpec)
        .withDistribution(distribution)
        .build();
  }

  @Override
  protected TableOperations newOps() {
    // todo: implement this method when we have the Iceberg table operations.
    throw new UnsupportedOperationException("IcebergTable does not support TableOperations.");
  }

  /** A builder class for constructing IcebergTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, IcebergTable> {

    private String location;

    public Builder withLocation(String location) {
      this.location = location;
      return this;
    }

    /**
     * Internal method to build an IcebergTable instance using the provided values.
     *
     * @return A new IcebergTable instance with the configured values.
     */
    @Override
    protected IcebergTable internalBuild() {
      IcebergTable icebergTable = new IcebergTable();
      icebergTable.name = name;
      icebergTable.comment = comment;
      icebergTable.properties =
          properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      icebergTable.auditInfo = auditInfo;
      icebergTable.columns = columns;
      if (null != location) {
        icebergTable.location = location;
      } else {
        icebergTable.location = icebergTable.properties.get(PROP_LOCATION);
      }
      icebergTable.partitioning = partitioning;
      icebergTable.distribution = distribution;
      icebergTable.sortOrders = sortOrders;
      if (null != comment) {
        icebergTable.properties.putIfAbsent(ICEBERG_COMMENT_FIELD_NAME, comment);
      }
      String provider = icebergTable.properties.get(PROP_PROVIDER);
      if (provider != null && !DEFAULT_ICEBERG_PROVIDER.equalsIgnoreCase(provider)) {
        throw new IllegalArgumentException("Unsupported format in USING: " + provider);
      }
      return icebergTable;
    }
  }
}
