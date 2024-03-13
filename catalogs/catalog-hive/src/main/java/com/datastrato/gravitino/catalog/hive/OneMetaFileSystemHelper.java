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
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.utils.OneMetaConstants.CORE_SITE_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.HDFS_SITE_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.KEYTAB_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.MOUNT_TABLE_PATH;
import static com.datastrato.gravitino.utils.OneMetaConstants.PRINCIPAL;

import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneMetaFileSystemHelper implements Serializable {
  public static final Logger LOG = LoggerFactory.getLogger(OneMetaFileSystemHelper.class);
  private static final long serialVersionUID = -7579536335018210975L;

  private FileSystem fs;

  public static HdfsFileServiceBuilder newBuilder() {
    return new HdfsFileServiceBuilder();
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public void closeFS(FileSystem fs) {
    if (fs != null) {
      IOUtils.closeQuietly(fs);
    }
  }

  public void close() {
    closeFS(fs);
  }

  public static class HdfsFileServiceBuilder {

    public static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileServiceBuilder.class);

    public OneMetaFileSystemHelper build() {
      try {
        UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB_PATH);
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        LOG.info(String.format("Success login ugi:%s", ugi.toString()));
      } catch (IOException e) {
        LOG.error("Kerberos authentication error", e);
        throw new RuntimeException("Kerberos authentication error!");
      }

      Configuration conf = new Configuration();
      conf.addResource(new Path(CORE_SITE_PATH));
      conf.addResource(new Path(HDFS_SITE_PATH));
      conf.addResource(new Path(MOUNT_TABLE_PATH));
      LOG.info("FileSystemHelper : 初始化Configuration成功");

      // 多实例
      FileSystem fs = null;
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        LOG.error("Get FileSystem error!", e);
        throw new RuntimeException("Get FileSystem error!");
      }
      return new OneMetaFileSystemHelper(fs);
    }
  }

  private OneMetaFileSystemHelper(FileSystem fs) {
    this.fs = fs;
  }
}
