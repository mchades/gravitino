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
package org.apache.gravitino.catalog;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code ManagedFunctionOperations} provides an implementation of {@link FunctionCatalog} using
 * EntityStore.
 */
public class ManagedFunctionOperations implements FunctionCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedFunctionOperations.class);
  private static final int INIT_VERSION = 0;
  private static final String LATEST_VERSION = "-1";

  private final EntityStore store;
  private final IdGenerator idGenerator;

  public ManagedFunctionOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    try {
      // List all entities in the namespace
      List<FunctionEntity> allFunctions =
          store.list(namespace, FunctionEntity.class, Entity.EntityType.FUNCTION);

      // Deduplicate by function name (keep only latest version of each function)
      return allFunctions.stream()
          .collect(
              Collectors.toMap(
                  entity -> extractBaseName(entity.name()),
                  entity -> entity,
                  (existing, replacement) ->
                      existing.version() > replacement.version() ? existing : replacement))
          .values()
          .stream()
          .map(entity -> NameIdentifierUtil.ofFunction(namespace, extractBaseName(entity.name())))
          .sorted()
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      LOG.error("Failed to list functions in namespace {}", namespace, e);
      throw new RuntimeException("Failed to list functions", e);
    }
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    // Get latest version
    NameIdentifier latestIdent = toVersionedIdentifier(ident, LATEST_VERSION);
    return getSpecificVersion(latestIdent, ident);
  }

  @Override
  public Function getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    if (version < 0) {
      throw new NoSuchFunctionVersionException(
          "Invalid version %d for function %s", version, ident);
    }

    NameIdentifier versionedIdent = toVersionedIdentifier(ident, String.valueOf(version));
    try {
      FunctionEntity entity =
          store.get(versionedIdent, Entity.EntityType.FUNCTION, FunctionEntity.class);
      if (entity == null) {
        throw new NoSuchFunctionVersionException(
            "Function version %d does not exist for %s", version, ident);
      }
      return toFunction(entity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchFunctionVersionException(
          "Function version %d does not exist for %s", version, ident);
    } catch (IOException e) {
      LOG.error("Failed to get function {} version {}", ident, version, e);
      throw new RuntimeException("Failed to get function", e);
    }
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Type returnType,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    validateIdentifier(ident);
    validateDefinitions(definitions);
    validateReturnType(functionType, returnType, null);

    FunctionEntity entity =
        FunctionEntity.builder()
            .withId(idGenerator.nextId())
            .withName(toVersionedName(ident.name(), INIT_VERSION))
            .withNamespace(ident.namespace())
            .withVersion(INIT_VERSION)
            .withFunctionType(functionType)
            .withDeterministic(deterministic)
            .withComment(comment)
            .withDefinitions(definitions)
            .withReturnType(returnType)
            .withReturnColumns(null)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      NameIdentifier versionedIdent = toVersionedIdentifier(ident, String.valueOf(INIT_VERSION));
      store.put(entity, false /* overwrite */);

      // Also store as latest version
      FunctionEntity latestEntity =
          copyWithName(entity, toVersionedName(ident.name(), LATEST_VERSION));
      NameIdentifier latestIdent = toVersionedIdentifier(ident, LATEST_VERSION);
      store.put(latestEntity, true /* overwrite */);

      return toFunction(entity);
    } catch (EntityAlreadyExistsException e) {
      throw new FunctionAlreadyExistsException("Function %s already exists", ident);
    } catch (IOException e) {
      LOG.error("Failed to register function {}", ident, e);
      throw new RuntimeException("Failed to register function", e);
    }
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionColumn[] returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    validateIdentifier(ident);
    validateDefinitions(definitions);
    validateReturnType(FunctionType.TABLE, null, returnColumns);

    FunctionEntity entity =
        FunctionEntity.builder()
            .withId(idGenerator.nextId())
            .withName(toVersionedName(ident.name(), INIT_VERSION))
            .withNamespace(ident.namespace())
            .withVersion(INIT_VERSION)
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(deterministic)
            .withComment(comment)
            .withDefinitions(definitions)
            .withReturnType(null)
            .withReturnColumns(returnColumns)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      NameIdentifier versionedIdent = toVersionedIdentifier(ident, String.valueOf(INIT_VERSION));
      store.put(entity, false /* overwrite */);

      // Also store as latest version
      FunctionEntity latestEntity =
          copyWithName(entity, toVersionedName(ident.name(), LATEST_VERSION));
      NameIdentifier latestIdent = toVersionedIdentifier(ident, LATEST_VERSION);
      store.put(latestEntity, true /* overwrite */);

      return toFunction(entity);
    } catch (EntityAlreadyExistsException e) {
      throw new FunctionAlreadyExistsException("Function %s already exists", ident);
    } catch (IOException e) {
      LOG.error("Failed to register function {}", ident, e);
      throw new RuntimeException("Failed to register function", e);
    }
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    if (changes == null || changes.length == 0) {
      throw new IllegalArgumentException("Changes cannot be null or empty");
    }
    validateIdentifier(ident);

    try {
      // Get current latest version
      NameIdentifier latestIdent = toVersionedIdentifier(ident, LATEST_VERSION);
      FunctionEntity currentEntity =
          store.get(latestIdent, Entity.EntityType.FUNCTION, FunctionEntity.class);
      if (currentEntity == null) {
        throw new NoSuchFunctionException("Function %s does not exist", ident);
      }

      // Apply changes
      FunctionEntity updatedEntity =
          applyChanges(currentEntity, changes, currentEntity.version() + 1);

      // Store new version
      NameIdentifier newVersionIdent =
          toVersionedIdentifier(ident, String.valueOf(updatedEntity.version()));
      store.put(updatedEntity, false /* overwrite */);

      // Update latest version
      FunctionEntity latestEntity =
          copyWithName(updatedEntity, toVersionedName(ident.name(), LATEST_VERSION));
      store.update(
          latestIdent, FunctionEntity.class, Entity.EntityType.FUNCTION, updater -> latestEntity);

      return toFunction(updatedEntity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchFunctionException("Function %s does not exist", ident);
    } catch (IOException e) {
      LOG.error("Failed to alter function {}", ident, e);
      throw new RuntimeException("Failed to alter function", e);
    }
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    validateIdentifier(ident);

    try {
      // Get latest version to find all versions
      NameIdentifier latestIdent = toVersionedIdentifier(ident, LATEST_VERSION);
      FunctionEntity latestEntity =
          store.get(latestIdent, Entity.EntityType.FUNCTION, FunctionEntity.class);
      if (latestEntity == null) {
        return false;
      }

      // Delete all versions
      for (int v = 0; v <= latestEntity.version(); v++) {
        NameIdentifier versionIdent = toVersionedIdentifier(ident, String.valueOf(v));
        try {
          store.delete(versionIdent, Entity.EntityType.FUNCTION);
        } catch (NoSuchEntityException e) {
          // Version might not exist, continue
        }
      }

      // Delete latest version marker
      store.delete(latestIdent, Entity.EntityType.FUNCTION);

      return true;
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      LOG.error("Failed to drop function {}", ident, e);
      throw new RuntimeException("Failed to drop function", e);
    }
  }

  private Function getSpecificVersion(NameIdentifier versionedIdent, NameIdentifier originalIdent)
      throws NoSuchFunctionException {
    try {
      FunctionEntity entity = null;

      if (versionedIdent.name().endsWith("#" + LATEST_VERSION)) {
        // Try to get the latest version marker
        entity = store.get(versionedIdent, Entity.EntityType.FUNCTION, FunctionEntity.class);

        // If not found, try to find the highest numbered version
        if (entity == null) {
          List<FunctionEntity> allVersions =
              store.list(
                  originalIdent.namespace(), FunctionEntity.class, Entity.EntityType.FUNCTION);
          String baseName = originalIdent.name();
          entity =
              allVersions.stream()
                  .filter(e -> extractBaseName(e.name()).equals(baseName))
                  .filter(e -> !e.name().endsWith("#" + LATEST_VERSION))
                  .max((a, b) -> Integer.compare(a.version(), b.version()))
                  .orElse(null);
        }
      } else {
        entity = store.get(versionedIdent, Entity.EntityType.FUNCTION, FunctionEntity.class);
      }

      if (entity == null) {
        throw new NoSuchFunctionException("Function %s does not exist", originalIdent);
      }
      return toFunction(entity);
    } catch (NoSuchEntityException e) {
      throw new NoSuchFunctionException("Function %s does not exist", originalIdent);
    } catch (IOException e) {
      LOG.error("Failed to get function {}", originalIdent, e);
      throw new RuntimeException("Failed to get function", e);
    }
  }

  private FunctionEntity applyChanges(
      FunctionEntity current, FunctionChange[] changes, int newVersion) {
    String newComment = current.comment();
    List<FunctionDefinition> updatedDefinitions =
        new ArrayList<>(Arrays.asList(current.definitions()));

    for (FunctionChange change : changes) {
      if (change instanceof FunctionChange.UpdateComment) {
        newComment = ((FunctionChange.UpdateComment) change).newComment();
      } else if (change instanceof FunctionChange.AddDefinition) {
        FunctionDefinition definition = ((FunctionChange.AddDefinition) change).definition();
        updatedDefinitions.add(definition);
      } else if (change instanceof FunctionChange.RemoveDefinition) {
        FunctionParam[] params = ((FunctionChange.RemoveDefinition) change).parameters();
        updatedDefinitions.removeIf(def -> parametersMatch(def.parameters(), params));
      } else if (change instanceof FunctionChange.AddImpl) {
        FunctionChange.AddImpl addImpl = (FunctionChange.AddImpl) change;
        updateDefinitionImpl(
            updatedDefinitions,
            addImpl.parameters(),
            def -> addImplementation(def, addImpl.implementation()));
      } else if (change instanceof FunctionChange.UpdateImpl) {
        FunctionChange.UpdateImpl updateImpl = (FunctionChange.UpdateImpl) change;
        updateDefinitionImpl(
            updatedDefinitions,
            updateImpl.parameters(),
            def -> updateImplementation(def, updateImpl.runtime(), updateImpl.implementation()));
      } else if (change instanceof FunctionChange.RemoveImpl) {
        FunctionChange.RemoveImpl removeImpl = (FunctionChange.RemoveImpl) change;
        updateDefinitionImpl(
            updatedDefinitions,
            removeImpl.parameters(),
            def -> removeImplementation(def, removeImpl.runtime()));
      }
    }

    return FunctionEntity.builder()
        .withId(idGenerator.nextId())
        .withName(toVersionedName(extractBaseName(current.name()), newVersion))
        .withNamespace(current.namespace())
        .withVersion(newVersion)
        .withFunctionType(current.functionType())
        .withDeterministic(current.deterministic())
        .withComment(newComment)
        .withDefinitions(updatedDefinitions.toArray(new FunctionDefinition[0]))
        .withReturnType(current.returnType())
        .withReturnColumns(current.returnColumns())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(current.auditInfo().creator())
                .withCreateTime(current.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private FunctionDefinition addImplementation(FunctionDefinition def, FunctionImpl impl) {
    List<FunctionImpl> impls = new ArrayList<>(Arrays.asList(def.impls()));
    impls.add(impl);
    return FunctionDefinitions.of(def.parameters(), impls.toArray(new FunctionImpl[0]));
  }

  private FunctionDefinition updateImplementation(
      FunctionDefinition def, FunctionImpl.RuntimeType runtime, FunctionImpl impl) {
    FunctionImpl[] impls = def.impls();
    FunctionImpl[] updated = Arrays.copyOf(impls, impls.length);
    for (int i = 0; i < updated.length; i++) {
      if (updated[i].runtime().equals(runtime)) {
        updated[i] = impl;
        break;
      }
    }
    return FunctionDefinitions.of(def.parameters(), updated);
  }

  private FunctionDefinition removeImplementation(
      FunctionDefinition def, FunctionImpl.RuntimeType runtime) {
    List<FunctionImpl> impls = new ArrayList<>(Arrays.asList(def.impls()));
    impls.removeIf(impl -> impl.runtime().equals(runtime));
    if (impls.isEmpty()) {
      throw new IllegalArgumentException("Cannot remove all implementations from a definition");
    }
    return FunctionDefinitions.of(def.parameters(), impls.toArray(new FunctionImpl[0]));
  }

  private void updateDefinitionImpl(
      List<FunctionDefinition> definitions,
      FunctionParam[] params,
      java.util.function.Function<FunctionDefinition, FunctionDefinition> updater) {
    for (int i = 0; i < definitions.size(); i++) {
      if (parametersMatch(definitions.get(i).parameters(), params)) {
        definitions.set(i, updater.apply(definitions.get(i)));
        return;
      }
    }
    throw new IllegalArgumentException("Definition with specified parameters not found");
  }

  private boolean parametersMatch(FunctionParam[] existing, FunctionParam[] target) {
    if (existing == null && target == null) return true;
    if (existing == null || target == null) return false;
    if (existing.length != target.length) return false;

    for (int i = 0; i < existing.length; i++) {
      if (!existing[i].name().equals(target[i].name())
          || !existing[i].dataType().equals(target[i].dataType())) {
        return false;
      }
    }
    return true;
  }

  private NameIdentifier toVersionedIdentifier(NameIdentifier ident, String version) {
    return NameIdentifierUtil.ofFunction(
        ident.namespace().levels(), toVersionedName(ident.name(), version));
  }

  private String toVersionedName(String baseName, int version) {
    return baseName + "#" + version;
  }

  private String toVersionedName(String baseName, String version) {
    if (baseName.contains("#")) {
      baseName = extractBaseName(baseName);
    }
    return baseName + "#" + version;
  }

  private String extractBaseName(String versionedName) {
    int idx = versionedName.lastIndexOf('#');
    return idx > 0 ? versionedName.substring(0, idx) : versionedName;
  }

  private FunctionEntity copyWithName(FunctionEntity entity, String newName) {
    return FunctionEntity.builder()
        .withId(entity.id())
        .withName(newName)
        .withNamespace(entity.namespace())
        .withVersion(entity.version())
        .withFunctionType(entity.functionType())
        .withDeterministic(entity.deterministic())
        .withComment(entity.comment())
        .withDefinitions(entity.definitions())
        .withReturnType(entity.returnType())
        .withReturnColumns(entity.returnColumns())
        .withAuditInfo(entity.auditInfo())
        .build();
  }

  private Function toFunction(FunctionEntity entity) {
    return new ManagedFunction(entity);
  }

  private void validateIdentifier(NameIdentifier ident) {
    if (ident == null) {
      throw new IllegalArgumentException("Function identifier cannot be null");
    }
    if (ident.namespace() == null || ident.namespace().length() < 3) {
      throw new IllegalArgumentException(
          "Function identifier must include metalake/catalog/schema");
    }
  }

  private void validateDefinitions(FunctionDefinition[] definitions) {
    if (definitions == null || definitions.length == 0) {
      throw new IllegalArgumentException("Function definitions cannot be null or empty");
    }
    for (FunctionDefinition definition : definitions) {
      if (definition == null) {
        throw new IllegalArgumentException("Function definition cannot be null");
      }
      FunctionImpl[] impls = definition.impls();
      if (impls == null || impls.length == 0) {
        throw new IllegalArgumentException("Function definition must contain implementations");
      }
    }
  }

  private void validateReturnType(
      FunctionType type, Type returnType, FunctionColumn[] returnColumns) {
    if (type == FunctionType.TABLE) {
      if (returnColumns == null || returnColumns.length == 0) {
        throw new IllegalArgumentException("Table function must define return columns");
      }
    } else if (returnType == null) {
      throw new IllegalArgumentException("Scalar/Aggregate function must define return type");
    }
  }

  /** Managed function implementation that wraps a FunctionEntity. */
  private static class ManagedFunction implements Function {
    private final FunctionEntity entity;

    ManagedFunction(FunctionEntity entity) {
      this.entity = entity;
    }

    @Override
    public String name() {
      return extractBaseName(entity.name());
    }

    @Override
    public FunctionType functionType() {
      return entity.functionType();
    }

    @Override
    public boolean deterministic() {
      return entity.deterministic();
    }

    @Override
    public String comment() {
      return entity.comment();
    }

    @Override
    public Type returnType() {
      return entity.functionType() == FunctionType.TABLE ? null : entity.returnType();
    }

    @Override
    public FunctionColumn[] returnColumns() {
      return entity.functionType() == FunctionType.TABLE && entity.returnColumns() != null
          ? Arrays.copyOf(entity.returnColumns(), entity.returnColumns().length)
          : new FunctionColumn[0];
    }

    @Override
    public FunctionDefinition[] definitions() {
      return Arrays.copyOf(entity.definitions(), entity.definitions().length);
    }

    @Override
    public int version() {
      return entity.version();
    }

    @Override
    public org.apache.gravitino.Audit auditInfo() {
      return entity.auditInfo();
    }

    private static String extractBaseName(String versionedName) {
      int idx = versionedName.lastIndexOf('#');
      return idx > 0 ? versionedName.substring(0, idx) : versionedName;
    }
  }
}
