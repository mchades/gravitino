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

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.datastrato.gravitino.storage.relational.mapper.GroupMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.GroupRoleRelMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.RoleMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SecurableObjectMapper;
import com.datastrato.gravitino.storage.relational.mapper.TableMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.TopicMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.UserMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.UserRoleRelMapper;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.datastrato.gravitino.utils.NameIdentifierUtil;
import com.datastrato.gravitino.utils.NamespaceUtil;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The service class for metalake metadata. It provides the basic database operations for metalake.
 */
public class MetalakeMetaService {
  private static final MetalakeMetaService INSTANCE = new MetalakeMetaService();

  public static MetalakeMetaService getInstance() {
    return INSTANCE;
  }

  private MetalakeMetaService() {}

  public List<BaseMetalake> listMetalakes() {
    List<MetalakePO> metalakePOS =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, MetalakeMetaMapper::listMetalakePOs);
    return POConverters.fromMetalakePOs(metalakePOS);
  }

  public Long getMetalakeIdByName(String metalakeName) {
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }
    return metalakeId;
  }

  public BaseMetalake getMetalakeByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkMetalake(ident);
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (metalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }
    return POConverters.fromMetalakePO(metalakePO);
  }

  // Metalake may be deleted, so the MetalakePO may be null.
  public MetalakePO getMetalakePOById(Long id) {
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaById(id));
    return metalakePO;
  }

  public void insertMetalake(BaseMetalake baseMetalake, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkMetalake(baseMetalake.nameIdentifier());
      SessionUtils.doWithCommit(
          MetalakeMetaMapper.class,
          mapper -> {
            MetalakePO po = POConverters.initializeMetalakePOWithVersion(baseMetalake);
            if (overwrite) {
              mapper.insertMetalakeMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertMetalakeMeta(po);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.METALAKE, baseMetalake.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> BaseMetalake updateMetalake(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkMetalake(ident);
    MetalakePO oldMetalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (oldMetalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }

    BaseMetalake oldMetalakeEntity = POConverters.fromMetalakePO(oldMetalakePO);
    BaseMetalake newMetalakeEntity = (BaseMetalake) updater.apply((E) oldMetalakeEntity);
    Preconditions.checkArgument(
        Objects.equals(oldMetalakeEntity.id(), newMetalakeEntity.id()),
        "The updated metalake entity id: %s should be same with the metalake entity id before: %s",
        newMetalakeEntity.id(),
        oldMetalakeEntity.id());
    MetalakePO newMetalakePO =
        POConverters.updateMetalakePOWithVersion(oldMetalakePO, newMetalakeEntity);
    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              MetalakeMetaMapper.class,
              mapper -> mapper.updateMetalakeMeta(newMetalakePO, oldMetalakePO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.METALAKE, newMetalakeEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newMetalakeEntity;
    } else {
      throw new IOException("Failed to update the entity: " + ident);
    }
  }

  public boolean deleteMetalake(NameIdentifier ident, boolean cascade) {
    NameIdentifierUtil.checkMetalake(ident);
    Long metalakeId = getMetalakeIdByName(ident.name());
    if (metalakeId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    CatalogMetaMapper.class,
                    mapper -> mapper.softDeleteCatalogMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TableMetaMapper.class,
                    mapper -> mapper.softDeleteTableMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.softDeleteFilesetMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.softDeleteFilesetVersionsByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TopicMetaMapper.class,
                    mapper -> mapper.softDeleteTopicMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper -> mapper.softDeleteUserRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserMetaMapper.class,
                    mapper -> mapper.softDeleteUserMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupRoleRelMapper.class,
                    mapper -> mapper.softDeleteGroupRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupMetaMapper.class,
                    mapper -> mapper.softDeleteGroupMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    RoleMetaMapper.class,
                    mapper -> mapper.softDeleteRoleMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SecurableObjectMapper.class,
                    mapper -> mapper.softDeleteRoleMetasByMetalakeId(metalakeId)));
      } else {
        List<CatalogEntity> catalogEntities =
            CatalogMetaService.getInstance()
                .listCatalogsByNamespace(NamespaceUtil.ofCatalog(ident.name()));
        if (!catalogEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", ident);
        }
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper -> mapper.softDeleteUserRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    UserMetaMapper.class,
                    mapper -> mapper.softDeleteUserMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupRoleRelMapper.class,
                    mapper -> mapper.softDeleteGroupRoleRelByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    GroupMetaMapper.class,
                    mapper -> mapper.softDeleteGroupMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    RoleMetaMapper.class,
                    mapper -> mapper.softDeleteRoleMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SecurableObjectMapper.class,
                    mapper -> mapper.softDeleteRoleMetasByMetalakeId(metalakeId)));
      }
    }
    return true;
  }

  public int deleteMetalakeMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        MetalakeMetaMapper.class,
        mapper -> {
          return mapper.deleteMetalakeMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }
}
