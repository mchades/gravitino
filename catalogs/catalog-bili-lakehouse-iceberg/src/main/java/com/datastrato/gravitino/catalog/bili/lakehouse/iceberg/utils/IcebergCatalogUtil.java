/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.bili.lakehouse.iceberg.utils;

import com.datastrato.gravitino.catalog.bili.lakehouse.iceberg.IcebergCatalogBackend;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogUtil.class);

  private static HiveCatalog loadHiveCatalog(Map<String, String> properties) {
    HiveCatalog hiveCatalog = new HiveCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    hiveCatalog.setConf(hdfsConfiguration);
    hiveCatalog.initialize("hive", properties);
    return hiveCatalog;
  }

  public static Catalog loadCatalogBackend(String catalogType) {
    return loadCatalogBackend(catalogType, Collections.emptyMap());
  }

  public static Catalog loadCatalogBackend(String catalogType, Map<String, String> properties) {
    LOG.info("Load catalog backend of {}", catalogType);
    switch (IcebergCatalogBackend.valueOf(catalogType.toUpperCase())) {
      case HIVE:
        return loadHiveCatalog(properties);
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }

  private IcebergCatalogUtil() {}
}
