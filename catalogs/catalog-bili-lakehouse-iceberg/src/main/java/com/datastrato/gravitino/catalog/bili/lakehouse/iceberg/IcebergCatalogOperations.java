/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.bili.lakehouse.iceberg;

import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.gravitino.utils.OneMetaConstants.CORE_SITE_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.HDFS_SITE_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.HIVE_SITE_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.MOUNT_TABLE_PATH;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.utils.MapUtils;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.sdk.HiveCatalogUtils;
import org.apache.iceberg.sdk.auth.AuthUtils;
import org.apache.iceberg.sdk.auth.HdfsAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with the Iceberg catalog in Gravitino. */
public class IcebergCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final String ICEBERG_TABLE_DOES_NOT_EXIST_MSG = "Iceberg table does not exist: %s";

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogOperations.class);

  private IcebergCatalogPropertiesMetadata icebergCatalogPropertiesMetadata;

  private IcebergTablePropertiesMetadata icebergTablePropertiesMetadata;

  private IcebergSchemaPropertiesMetadata icebergSchemaPropertiesMetadata;

  private CatalogInfo info;

  private Configuration icebergSdkConf = null;

  /**
   * Initializes the Iceberg catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Iceberg catalog operations.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(Map<String, String> conf, CatalogInfo info) throws RuntimeException {
    this.info = info;
    // Key format like gravitino.bypass.a.b
    Map<String, String> prefixMap = MapUtils.getPrefixMap(conf, CATALOG_BYPASS_PREFIX);

    this.icebergCatalogPropertiesMetadata = new IcebergCatalogPropertiesMetadata();
    // Hold keys that lie in GRAVITINO_CONFIG_TO_ICEBERG
    Map<String, String> gravitinoConfig =
        this.icebergCatalogPropertiesMetadata.transformProperties(conf);

    Map<String, String> resultConf = Maps.newHashMap(prefixMap);
    resultConf.putAll(gravitinoConfig);

    IcebergConfig icebergConfig = new IcebergConfig(resultConf);

    this.icebergTablePropertiesMetadata = new IcebergTablePropertiesMetadata();
    this.icebergSchemaPropertiesMetadata = new IcebergSchemaPropertiesMetadata();
  }

  /** Closes the Iceberg catalog and releases the associated client pool. */
  @Override
  public void close() {}

  /**
   * Lists the schemas under the given namespace.
   *
   * @param namespace The namespace to list the schemas for.
   * @return An array of {@link NameIdentifier} representing the schemas.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param properties The properties for the schema.
   * @return The created {@link IcebergSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public IcebergSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Loads the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to load.
   * @return The loaded {@link IcebergSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public IcebergSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Alters the schema with the provided identifier according to the specified changes.
   *
   * @param ident The identifier of the schema to alter.
   * @param changes The changes to apply to the schema.
   * @return The altered {@link IcebergSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public IcebergSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Drops the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to drop.
   * @param cascade If set to true, drops all the tables in the schema as well.
   * @return true if the schema was dropped successfully, false otherwise.
   * @throws NonEmptySchemaException If the schema is not empty and 'cascade' is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Lists all the tables under the specified namespace.
   *
   * @param namespace The namespace to list tables for.
   * @return An array of {@link NameIdentifier} representing the tables in the namespace.
   * @throws NoSuchSchemaException If the schema with the provided namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Loads a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded IcebergTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the Iceberg.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Apply the {@link TableChange change} to an existing Iceberg table.
   *
   * @param tableIdent The identifier of the table to alter.
   * @param changes The changes to apply to the table.
   * @return This method always throws UnsupportedOperationException.
   * @throws NoSuchTableException This exception will not be thrown in this method.
   * @throws IllegalArgumentException This exception will not be thrown in this method.
   */
  @Override
  public Table alterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  private Table internalUpdateTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Perform name change operations on the Iceberg.
   *
   * @param tableIdent tableIdent of this table.
   * @param renameTable Table Change to modify the table name.
   * @return Returns the table for Iceberg.
   * @throws NoSuchTableException
   * @throws IllegalArgumentException
   */
  private Table renameTable(NameIdentifier tableIdent, TableChange.RenameTable renameTable)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Drops a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Creates a new table in the Iceberg.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitioning The partitioning for the new table.
   * @param indexes The indexes for the new table.
   * @return The newly created IcebergTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }

  /**
   * Purges a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }
  /**
   * Purges a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTableOneMeta(NameIdentifier tableIdent) {
    // use iceberg sdk
    try {
      HdfsAuthentication hdfsAuthentication = AuthUtils.createHdfsAuthentication(icebergSdkConf);
      hdfsAuthentication.doAs(
          () -> {
            TableIdentifier identifier = TableIdentifier.of(tableIdent.name(), tableIdent.name());
            HiveCatalog hiveCatalog = HiveCatalogUtils.createHiveCatalog(icebergSdkConf);
            hiveCatalog.dropTable(identifier, true);
            return null;
          });
      hdfsAuthentication.close();
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.warn("Iceberg table {} does not exist", tableIdent.name());
      return false;
    } catch (Throwable e) {
      LOG.info("Purge Iceberg table Error : {}", tableIdent.name());
    }
    return true;
  }

  private Configuration createDefaultConfiguration() {
    Configuration defaultConf = new Configuration();
    defaultConf.addResource(new Path(CORE_SITE_PATH));
    defaultConf.addResource(new Path(HDFS_SITE_PATH));
    defaultConf.addResource(new Path(HIVE_SITE_PATH));
    defaultConf.addResource(new Path(MOUNT_TABLE_PATH));
    return defaultConf;
  }

  // TODO. We should figure out a better way to get the current user from servlet container.
  private static String currentUser() {
    return System.getProperty("user.name");
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return icebergTablePropertiesMetadata;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return icebergCatalogPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return icebergSchemaPropertiesMetadata;
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Iceberg catalog doesn't support fileset related operations");
  }

  @Override
  public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Iceberg catalog doesn't support topic related operations");
  }
}
