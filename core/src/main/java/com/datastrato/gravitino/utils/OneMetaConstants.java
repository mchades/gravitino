/**
 * Copyright (C) 2016-2023 Expedia, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastrato.gravitino.utils;

public class OneMetaConstants {

  public static final String ROLE_NAME = "onemeta.role.name";

  public static final String CORE_SITE_PATH = "/data/app/onemeta/conf/core-site.xml";
  public static final String HDFS_SITE_PATH = "/data/app/onemeta/conf/hdfs-site.xml";
  public static final String HIVE_SITE_PATH = "/data/app/onemeta/conf/hive-site.xml";
  public static final String MOUNT_TABLE_PATH = "/data/app/onemeta/conf/mount-table.xml";

  public static final String KEYTAB_PATH = "/etc/security/keytabs/hive.keytab";
  public static final String PRINCIPAL = "hive@BILIBILI.CO";

  // metrics
  public static final String METRIC_ICEBERG_ACCESS_TIME = "onemeta_iceberg_access_time_ms";

  public static final String METRIC_DISPATCHER_ACCESS_TIME = "onemeta_dispatcher_access_time_ms";

  public static final String METRIC_HMS_ACCESS_TIME = "onemeta_hms_access_time_ms";

  public static final String METRIC_SERVICE_ACCESS_TIME = "onemeta_service_process_time_ms";

  public static final String METRIC_ERROR_COUNTER = "sqlscan_error_counter";

  public static final String METRIC_COUNTER = "sqlscan_counter";
  public static final String METRIC_OPEN_CONNECTIONS = "open_connections";
}
