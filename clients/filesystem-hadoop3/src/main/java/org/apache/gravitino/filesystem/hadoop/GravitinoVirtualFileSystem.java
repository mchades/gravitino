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
package org.apache.gravitino.filesystem.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.file.Fileset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Apache Gravitino server, and creates an independent file system for it to act as an agent for
 * users to access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger Logger = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private String metalakeName;
  private Cache<NameIdentifier, Pair<Fileset, FileSystem>> filesetCache;
  private ScheduledThreadPoolExecutor scheduler;

  // The pattern is used to match gvfs path. The scheme prefix (gvfs://fileset) is optional.
  // The following path can be match:
  //     gvfs://fileset/fileset_catalog/fileset_schema/fileset1/file.txt
  //     /fileset_catalog/fileset_schema/fileset1/sub_dir/
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$");
  private static final String SLASH = "/";

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }

    int maxCapacity =
        configuration.getInt(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT);
    Preconditions.checkArgument(
        maxCapacity > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY);

    long evictionMillsAfterAccess =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT);
    Preconditions.checkArgument(
        evictionMillsAfterAccess > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY);

    initializeCache(maxCapacity, evictionMillsAfterAccess);

    this.metalakeName =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    initializeClient(configuration);

    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  @VisibleForTesting
  Cache<NameIdentifier, Pair<Fileset, FileSystem>> getFilesetCache() {
    return filesetCache;
  }

  private void initializeCache(int maxCapacity, long expireAfterAccess) {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.scheduler = new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory());

    this.filesetCache =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            .expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS)
            .scheduler(Scheduler.forScheduledExecutorService(scheduler))
            .removalListener(
                (key, value, cause) -> {
                  try {
                    Pair<Fileset, FileSystem> pair = (Pair<Fileset, FileSystem>) value;
                    if (pair != null && pair.getRight() != null) pair.getRight().close();
                  } catch (IOException e) {
                    Logger.error("Cannot close the file system for fileset: {}", key, e);
                  }
                })
            .build();
  }

  private ThreadFactory newDaemonThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("gvfs-cache-cleaner" + "-%d")
        .build();
  }

  private void initializeClient(Configuration configuration) {
    // initialize the Gravitino client
    String serverUri =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);

    String authType =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
            GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
    if (authType.equalsIgnoreCase(GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE)) {
      this.client =
          GravitinoClient.builder(serverUri).withMetalake(metalakeName).withSimpleAuth().build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE)) {
      String authServerUri =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
          authServerUri);

      String credential =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
          credential);

      String path =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY,
          path);

      String scope =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY,
          scope);

      DefaultOAuth2TokenProvider authDataProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(authServerUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();

      this.client =
          GravitinoClient.builder(serverUri)
              .withMetalake(metalakeName)
              .withOAuth(authDataProvider)
              .build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE)) {
      String principal =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
          principal);
      String keytabFilePath =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration
                  .FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY);
      KerberosTokenProvider authDataProvider;
      if (StringUtils.isNotBlank(keytabFilePath)) {
        // Using principal and keytab to create auth provider
        authDataProvider =
            KerberosTokenProvider.builder()
                .withClientPrincipal(principal)
                .withKeyTabFile(new File(keytabFilePath))
                .build();
      } else {
        // Using ticket cache to create auth provider
        authDataProvider = KerberosTokenProvider.builder().withClientPrincipal(principal).build();
      }
      this.client =
          GravitinoClient.builder(serverUri)
              .withMetalake(metalakeName)
              .withKerberosAuth(authDataProvider)
              .build();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported authentication type: %s for %s.",
              authType, GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
    }
  }

  private void checkAuthConfig(String authType, String configKey, String configValue) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue),
        "%s should not be null if %s is set to %s.",
        configKey,
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        authType);
  }

  private String getVirtualLocation(NameIdentifier identifier, boolean withScheme) {
    return String.format(
        "%s/%s/%s/%s",
        withScheme ? GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX : "",
        identifier.namespace().level(1),
        identifier.namespace().level(2),
        identifier.name());
  }

  @VisibleForTesting
  Path getActualPathByIdentifier(
      NameIdentifier identifier, Pair<Fileset, FileSystem> filesetPair, Path path) {
    String virtualPath = path.toString();
    boolean withScheme =
        virtualPath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX);
    String virtualLocation = getVirtualLocation(identifier, withScheme);
    String storageLocation = filesetPair.getLeft().storageLocation();
    try {
      if (checkMountsSingleFile(filesetPair)) {
        Preconditions.checkArgument(
            virtualPath.equals(virtualLocation),
            "Path: %s should be same with the virtual prefix: %s, because the fileset only mounts a single file.",
            virtualPath,
            virtualLocation);

        return new Path(storageLocation);
      } else {
        // if the storage location ends with "/",
        // we should handle the conversion specially
        if (storageLocation.endsWith(SLASH)) {
          String subPath = virtualPath.substring(virtualLocation.length());
          // For example, if the virtual path is `gvfs://fileset/catalog/schema/test_fileset/ttt`,
          // and the storage location is `hdfs://cluster:8020/user/`,
          // we should replace `gvfs://fileset/catalog/schema/test_fileset` with
          // `hdfs://localhost:8020/user` which truncates the tailing slash.
          // If the storage location is `hdfs://cluster:8020/user`,
          // we can replace `gvfs://fileset/catalog/schema/test_fileset` with
          // `hdfs://localhost:8020/user` directly.
          if (subPath.startsWith(SLASH)) {
            return new Path(
                virtualPath.replaceFirst(
                    virtualLocation, storageLocation.substring(0, storageLocation.length() - 1)));
          } else {
            return new Path(virtualPath.replaceFirst(virtualLocation, storageLocation));
          }
        } else {
          return new Path(virtualPath.replaceFirst(virtualLocation, storageLocation));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Cannot resolve path: %s to actual storage path, exception:", path), e);
    }
  }

  private boolean checkMountsSingleFile(Pair<Fileset, FileSystem> filesetPair) {
    try {
      return filesetPair
          .getRight()
          .getFileStatus(new Path(filesetPair.getLeft().storageLocation()))
          .isFile();
    } catch (FileNotFoundException e) {
      // We should always return false here, same with the logic in `FileSystem.isFile(Path f)`.
      return false;
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Cannot check whether the fileset: %s mounts a single file, exception: %s",
              filesetPair.getLeft().name(), e.getMessage()),
          e);
    }
  }

  @VisibleForTesting
  FileStatus convertFileStatusPathPrefix(
      FileStatus fileStatus, String actualPrefix, String virtualPrefix) {
    String filePath = fileStatus.getPath().toString();
    Preconditions.checkArgument(
        filePath.startsWith(actualPrefix),
        "Path %s doesn't start with prefix \"%s\".",
        filePath,
        actualPrefix);
    // if the storage location is end with "/",
    // we should truncate this to avoid replace issues.
    Path path =
        new Path(
            filePath.replaceFirst(
                actualPrefix.endsWith(SLASH) && !virtualPrefix.endsWith(SLASH)
                    ? actualPrefix.substring(0, actualPrefix.length() - 1)
                    : actualPrefix,
                virtualPrefix));
    fileStatus.setPath(path);

    return fileStatus;
  }

  @VisibleForTesting
  NameIdentifier extractIdentifier(URI virtualUri) {
    String virtualPath = virtualUri.toString();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(virtualPath),
        "Uri which need be extracted cannot be null or empty.");

    Matcher matcher = IDENTIFIER_PATTERN.matcher(virtualPath);
    Preconditions.checkArgument(
        matcher.matches() && matcher.groupCount() == 3,
        "URI %s doesn't contains valid identifier",
        virtualPath);

    return NameIdentifier.of(metalakeName, matcher.group(1), matcher.group(2), matcher.group(3));
  }

  private FilesetContext getFilesetContext(Path virtualPath) {
    NameIdentifier identifier = extractIdentifier(virtualPath.toUri());
    Pair<Fileset, FileSystem> pair = filesetCache.get(identifier, this::constructNewFilesetPair);
    Preconditions.checkState(
        pair != null,
        "Cannot get the pair of fileset instance and actual file system for %s",
        identifier);
    Path actualPath = getActualPathByIdentifier(identifier, pair, virtualPath);
    return FilesetContext.builder()
        .withIdentifier(identifier)
        .withFileset(pair.getLeft())
        .withFileSystem(pair.getRight())
        .withActualPath(actualPath)
        .build();
  }

  private Pair<Fileset, FileSystem> constructNewFilesetPair(NameIdentifier identifier) {
    // Always create a new file system instance for the fileset.
    // Therefore, users cannot bypass gvfs and use `FileSystem.get()` to directly obtain the
    // FileSystem
    try {
      Fileset fileset = loadFileset(identifier);
      URI storageUri = URI.create(fileset.storageLocation());
      FileSystem actualFileSystem = FileSystem.newInstance(storageUri, getConf());
      Preconditions.checkState(actualFileSystem != null, "Cannot get the actual file system");
      return Pair.of(fileset, actualFileSystem);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Cannot create file system for fileset: %s, exception: %s",
              identifier, e.getMessage()),
          e);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Cannot load fileset: %s from the server. exception: %s",
              identifier, e.getMessage()));
    }
  }

  private Fileset loadFileset(NameIdentifier identifier) {
    Catalog catalog = client.loadCatalog(identifier.namespace().level(1));
    return catalog
        .asFilesetCatalog()
        .loadFileset(NameIdentifier.of(identifier.namespace().level(2), identifier.name()));
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public synchronized void setWorkingDirectory(Path newDir) {
    FilesetContext context = getFilesetContext(newDir);
    context.getFileSystem().setWorkingDirectory(context.getActualPath());
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FilesetContext context = getFilesetContext(path);
    return context.getFileSystem().open(context.getActualPath(), bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    FilesetContext context = getFilesetContext(path);
    return context
        .getFileSystem()
        .create(
            context.getActualPath(),
            permission,
            overwrite,
            bufferSize,
            replication,
            blockSize,
            progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    FilesetContext context = getFilesetContext(path);
    return context.getFileSystem().append(context.getActualPath(), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // There are two cases that cannot be renamed:
    // 1. Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    // 2. Fileset only mounts a single file, the storage location of the fileset cannot be renamed;
    // Otherwise the metadata in the Gravitino server may be inconsistent.
    NameIdentifier srcIdentifier = extractIdentifier(src.toUri());
    NameIdentifier dstIdentifier = extractIdentifier(dst.toUri());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);

    FilesetContext srcFileContext = getFilesetContext(src);
    if (checkMountsSingleFile(
        Pair.of(srcFileContext.getFileset(), srcFileContext.getFileSystem()))) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot rename the fileset: %s which only mounts to a single file.", srcIdentifier));
    }

    FilesetContext dstFileContext = getFilesetContext(dst);
    return srcFileContext
        .getFileSystem()
        .rename(srcFileContext.getActualPath(), dstFileContext.getActualPath());
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    FilesetContext context = getFilesetContext(path);
    return context.getFileSystem().delete(context.getActualPath(), recursive);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FilesetContext context = getFilesetContext(path);
    FileStatus fileStatus = context.getFileSystem().getFileStatus(context.getActualPath());
    return convertFileStatusPathPrefix(
        fileStatus,
        new Path(context.getFileset().storageLocation()).toString(),
        getVirtualLocation(context.getIdentifier(), true));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FilesetContext context = getFilesetContext(path);
    FileStatus[] fileStatusResults = context.getFileSystem().listStatus(context.getActualPath());
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                convertFileStatusPathPrefix(
                    fileStatus,
                    new Path(context.getFileset().storageLocation()).toString(),
                    getVirtualLocation(context.getIdentifier(), true)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    FilesetContext context = getFilesetContext(path);
    return context.getFileSystem().mkdirs(context.getActualPath(), permission);
  }

  @Override
  public short getDefaultReplication(Path f) {
    FilesetContext context = getFilesetContext(f);
    return context.getFileSystem().getDefaultReplication(context.getActualPath());
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    FilesetContext context = getFilesetContext(f);
    return context.getFileSystem().getDefaultBlockSize(context.getActualPath());
  }

  @Override
  public synchronized void close() throws IOException {
    // close all actual FileSystems
    for (Pair<Fileset, FileSystem> filesetPair : filesetCache.asMap().values()) {
      try {
        filesetPair.getRight().close();
      } catch (IOException e) {
        // ignore
      }
    }
    filesetCache.invalidateAll();
    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }
    scheduler.shutdownNow();
    super.close();
  }
}
