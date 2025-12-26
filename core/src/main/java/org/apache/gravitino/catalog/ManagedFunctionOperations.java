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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.gravitino.Audit;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
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
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.IdGenerator;

/**
 * {@code ManagedFunctionOperations} provides an in-memory implementation of {@link
 * FunctionCatalog}.
 *
 * <p>The relational storage layer for functions is still a work in progress. This class keeps the
 * function metadata purely in memory so that the upper server and API layers can be implemented and
 * verified without depending on the storage layer.
 */
public class ManagedFunctionOperations implements FunctionCatalog {

  private static final int INIT_VERSION = 0;

  @SuppressWarnings("unused")
  private final EntityStore store;

  @SuppressWarnings("unused")
  private final IdGenerator idGenerator;

  private final ConcurrentMap<Namespace, ConcurrentMap<String, VersionedFunction>> functions;
  private final Lock lock;

  public ManagedFunctionOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.functions = new ConcurrentHashMap<>();
    this.lock = new ReentrantLock();
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    ConcurrentMap<String, VersionedFunction> namespaceFunctions = functions.get(namespace);
    if (namespaceFunctions == null || namespaceFunctions.isEmpty()) {
      return new NameIdentifier[0];
    }

    return namespaceFunctions.keySet().stream()
        .sorted()
        .map(name -> NameIdentifier.of(namespace, name))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    VersionedFunction versioned = versionedFunction(ident);
    if (versioned == null || versioned.latest() == null) {
      throw new NoSuchFunctionException("Function %s does not exist", ident);
    }
    return versioned.latest().toFunction();
  }

  @Override
  public Function getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    if (version < 0) {
      throw new NoSuchFunctionVersionException(
          "Invalid version %d for function %s", version, ident);
    }

    VersionedFunction versioned = versionedFunction(ident);
    if (versioned == null) {
      throw new NoSuchFunctionException("Function %s does not exist", ident);
    }

    FunctionRecord record = versioned.get(version);
    if (record == null) {
      throw new NoSuchFunctionVersionException(
          "Function version %d does not exist for %s", version, ident);
    }
    return record.toFunction();
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

    lock.lock();
    try {
      ConcurrentMap<String, VersionedFunction> namespaceFunctions =
          functions.computeIfAbsent(ident.namespace(), key -> new ConcurrentHashMap<>());
      if (namespaceFunctions.containsKey(ident.name())) {
        throw new FunctionAlreadyExistsException("Function %s already exists", ident);
      }

      VersionedFunction versioned = new VersionedFunction();
      FunctionRecord record =
          FunctionRecord.of(
              ident,
              INIT_VERSION,
              functionType,
              deterministic,
              comment,
              definitions,
              returnType,
              null,
              createAudit());
      versioned.put(record);
      namespaceFunctions.put(ident.name(), versioned);
      return record.toFunction();
    } finally {
      lock.unlock();
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

    lock.lock();
    try {
      ConcurrentMap<String, VersionedFunction> namespaceFunctions =
          functions.computeIfAbsent(ident.namespace(), key -> new ConcurrentHashMap<>());
      if (namespaceFunctions.containsKey(ident.name())) {
        throw new FunctionAlreadyExistsException("Function %s already exists", ident);
      }

      VersionedFunction versioned = new VersionedFunction();
      FunctionRecord record =
          FunctionRecord.of(
              ident,
              INIT_VERSION,
              FunctionType.TABLE,
              deterministic,
              comment,
              definitions,
              null,
              returnColumns,
              createAudit());
      versioned.put(record);
      namespaceFunctions.put(ident.name(), versioned);
      return record.toFunction();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    if (changes == null || changes.length == 0) {
      throw new IllegalArgumentException("Changes cannot be null or empty");
    }
    validateIdentifier(ident);

    lock.lock();
    try {
      VersionedFunction versioned = versionedFunction(ident);
      if (versioned == null || versioned.latest() == null) {
        throw new NoSuchFunctionException("Function %s does not exist", ident);
      }

      FunctionRecord current = versioned.latest();
      FunctionRecord updated =
          current.applyChanges(changes, versioned.nextVersion(), createAudit());
      versioned.put(updated);
      return updated.toFunction();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    validateIdentifier(ident);

    lock.lock();
    try {
      ConcurrentMap<String, VersionedFunction> namespaceFunctions =
          functions.get(ident.namespace());
      if (namespaceFunctions == null) {
        return false;
      }
      VersionedFunction removed = namespaceFunctions.remove(ident.name());
      if (namespaceFunctions.isEmpty()) {
        functions.remove(ident.namespace());
      }
      return removed != null;
    } finally {
      lock.unlock();
    }
  }

  private VersionedFunction versionedFunction(NameIdentifier ident) {
    ConcurrentMap<String, VersionedFunction> namespaceFunctions = functions.get(ident.namespace());
    return namespaceFunctions == null ? null : namespaceFunctions.get(ident.name());
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

  private BasicAudit createAudit() {
    Instant now = Instant.now();
    String user = System.getProperty("user.name", "gravitino");
    return new BasicAudit(user, now, user, now);
  }

  private static final class VersionedFunction {
    private final NavigableMap<Integer, FunctionRecord> versions = new java.util.TreeMap<>();

    FunctionRecord latest() {
      return versions.isEmpty() ? null : versions.lastEntry().getValue();
    }

    FunctionRecord get(int version) {
      return versions.get(version);
    }

    void put(FunctionRecord record) {
      versions.put(record.version, record);
    }

    int nextVersion() {
      return versions.isEmpty() ? INIT_VERSION + 1 : versions.lastKey() + 1;
    }
  }

  private static final class FunctionRecord {
    private final Namespace namespace;
    private final String name;
    private final FunctionType type;
    private final boolean deterministic;
    private final String comment;
    private final FunctionDefinition[] definitions;
    private final Type returnType;
    private final FunctionColumn[] returnColumns;
    private final int version;
    private final BasicAudit audit;

    private FunctionRecord(
        Namespace namespace,
        String name,
        FunctionType type,
        boolean deterministic,
        String comment,
        FunctionDefinition[] definitions,
        Type returnType,
        FunctionColumn[] returnColumns,
        int version,
        BasicAudit audit) {
      this.namespace = namespace;
      this.name = name;
      this.type = type;
      this.deterministic = deterministic;
      this.comment = comment;
      this.definitions = copyDefinitions(definitions);
      this.returnType = returnType;
      this.returnColumns = returnColumns == null ? null : copyColumns(returnColumns);
      this.version = version;
      this.audit = audit;
    }

    static FunctionRecord of(
        NameIdentifier ident,
        int version,
        FunctionType type,
        boolean deterministic,
        String comment,
        FunctionDefinition[] definitions,
        Type returnType,
        FunctionColumn[] returnColumns,
        BasicAudit audit) {
      return new FunctionRecord(
          ident.namespace(),
          ident.name(),
          type,
          deterministic,
          comment,
          definitions,
          returnType,
          returnColumns,
          version,
          audit);
    }

    FunctionRecord applyChanges(FunctionChange[] changes, int newVersion, BasicAudit newAudit) {
      String newComment = comment;
      List<FunctionDefinition> updated =
          new ArrayList<>(Arrays.asList(copyDefinitions(definitions)));

      for (FunctionChange change : changes) {
        if (change instanceof FunctionChange.UpdateComment) {
          newComment = ((FunctionChange.UpdateComment) change).newComment();
        } else if (change instanceof FunctionChange.AddDefinition) {
          FunctionDefinition definition = ((FunctionChange.AddDefinition) change).definition();
          updated.add(copyDefinition(definition));
        } else if (change instanceof FunctionChange.RemoveDefinition) {
          FunctionParam[] params = ((FunctionChange.RemoveDefinition) change).parameters();
          int idx = findDefinition(updated, params);
          if (idx < 0) {
            throw new IllegalArgumentException("Definition to remove not found");
          }
          updated.remove(idx);
        } else if (change instanceof FunctionChange.AddImpl) {
          FunctionChange.AddImpl addImpl = (FunctionChange.AddImpl) change;
          replaceDefinition(
              updated,
              addImpl.parameters(),
              existing -> {
                List<FunctionImpl> impls = new ArrayList<>(Arrays.asList(existing.impls()));
                boolean exists =
                    impls.stream()
                        .anyMatch(
                            impl -> impl.runtime().equals(addImpl.implementation().runtime()));
                if (exists) {
                  throw new IllegalArgumentException("Implementation runtime already exists");
                }
                impls.add(addImpl.implementation());
                return FunctionDefinitions.of(
                    existing.parameters(), impls.toArray(new FunctionImpl[0]));
              });
        } else if (change instanceof FunctionChange.UpdateImpl) {
          FunctionChange.UpdateImpl updateImpl = (FunctionChange.UpdateImpl) change;
          replaceDefinition(
              updated,
              updateImpl.parameters(),
              existing -> {
                FunctionImpl[] impls = existing.impls();
                FunctionImpl[] replaced = Arrays.copyOf(impls, impls.length);
                boolean found = false;
                for (int i = 0; i < replaced.length; i++) {
                  if (replaced[i].runtime().equals(updateImpl.runtime())) {
                    replaced[i] = updateImpl.implementation();
                    found = true;
                    break;
                  }
                }
                if (!found) {
                  throw new IllegalArgumentException("Implementation runtime to update not found");
                }
                return FunctionDefinitions.of(existing.parameters(), replaced);
              });
        } else if (change instanceof FunctionChange.RemoveImpl) {
          FunctionChange.RemoveImpl removeImpl = (FunctionChange.RemoveImpl) change;
          replaceDefinition(
              updated,
              removeImpl.parameters(),
              existing -> {
                List<FunctionImpl> impls = new ArrayList<>(Arrays.asList(existing.impls()));
                boolean removed =
                    impls.removeIf(impl -> impl.runtime().equals(removeImpl.runtime()));
                if (!removed) {
                  throw new IllegalArgumentException("Implementation runtime to remove not found");
                }
                if (impls.isEmpty()) {
                  throw new IllegalArgumentException(
                      "Definition must contain at least one implementation");
                }
                return FunctionDefinitions.of(
                    existing.parameters(), impls.toArray(new FunctionImpl[0]));
              });
        } else {
          throw new IllegalArgumentException(
              "Unsupported change type: " + change.getClass().getSimpleName());
        }
      }

      if (updated.isEmpty()) {
        throw new IllegalArgumentException("Function definitions cannot be empty after changes");
      }

      return new FunctionRecord(
          namespace,
          name,
          type,
          deterministic,
          newComment,
          updated.toArray(new FunctionDefinition[0]),
          returnType,
          returnColumns,
          newVersion,
          newAudit);
    }

    Function toFunction() {
      return new Managed(copy());
    }

    private FunctionRecord copy() {
      return new FunctionRecord(
          namespace,
          name,
          type,
          deterministic,
          comment,
          definitions,
          returnType,
          returnColumns,
          version,
          audit);
    }

    private static FunctionDefinition[] copyDefinitions(FunctionDefinition[] definitions) {
      FunctionDefinition[] copied = new FunctionDefinition[definitions.length];
      for (int i = 0; i < definitions.length; i++) {
        copied[i] = copyDefinition(definitions[i]);
      }
      return copied;
    }

    private static FunctionDefinition copyDefinition(FunctionDefinition definition) {
      FunctionParam[] params = definition.parameters();
      FunctionImpl[] impls = definition.impls();
      return FunctionDefinitions.of(
          params == null ? new FunctionParam[0] : Arrays.copyOf(params, params.length),
          impls == null ? new FunctionImpl[0] : Arrays.copyOf(impls, impls.length));
    }

    private static FunctionColumn[] copyColumns(FunctionColumn[] columns) {
      return columns == null ? null : Arrays.copyOf(columns, columns.length);
    }

    private static int findDefinition(
        List<FunctionDefinition> definitions, FunctionParam[] params) {
      for (int i = 0; i < definitions.size(); i++) {
        if (parametersMatch(definitions.get(i).parameters(), params)) {
          return i;
        }
      }
      return -1;
    }

    private static void replaceDefinition(
        List<FunctionDefinition> definitions,
        FunctionParam[] params,
        java.util.function.UnaryOperator<FunctionDefinition> updater) {
      int idx = findDefinition(definitions, params);
      if (idx < 0) {
        throw new IllegalArgumentException("Function definition for parameters not found");
      }
      definitions.set(idx, updater.apply(definitions.get(idx)));
    }

    private static boolean parametersMatch(FunctionParam[] existing, FunctionParam[] target) {
      if (existing == null && target == null) {
        return true;
      }
      if (existing == null || target == null) {
        return false;
      }
      if (existing.length != target.length) {
        return false;
      }
      for (int i = 0; i < existing.length; i++) {
        FunctionParam left = existing[i];
        FunctionParam right = target[i];
        if (!Objects.equals(left.name(), right.name())
            || !Objects.equals(left.dataType(), right.dataType())) {
          return false;
        }
      }
      return true;
    }
  }

  private static final class Managed implements Function {
    private final FunctionRecord record;

    private Managed(FunctionRecord record) {
      this.record = record;
    }

    @Override
    public String name() {
      return record.name;
    }

    @Override
    public FunctionType functionType() {
      return record.type;
    }

    @Override
    public boolean deterministic() {
      return record.deterministic;
    }

    @Override
    public String comment() {
      return record.comment;
    }

    @Override
    public Type returnType() {
      return record.type == FunctionType.TABLE ? null : record.returnType;
    }

    @Override
    public FunctionColumn[] returnColumns() {
      if (record.type != FunctionType.TABLE || record.returnColumns == null) {
        return new FunctionColumn[0];
      }
      return Arrays.copyOf(record.returnColumns, record.returnColumns.length);
    }

    @Override
    public FunctionDefinition[] definitions() {
      return Arrays.copyOf(record.definitions, record.definitions.length);
    }

    @Override
    public int version() {
      return record.version;
    }

    @Override
    public Audit auditInfo() {
      return record.audit;
    }
  }

  private static final class BasicAudit implements Audit {
    private final String creator;
    private final Instant createTime;
    private final String lastModifier;
    private final Instant lastModifiedTime;

    private BasicAudit(
        String creator, Instant createTime, String lastModifier, Instant lastModifiedTime) {
      this.creator = creator;
      this.createTime = createTime;
      this.lastModifier = lastModifier;
      this.lastModifiedTime = lastModifiedTime;
    }

    BasicAudit evolve() {
      Instant now = Instant.now();
      String modifier = System.getProperty("user.name", "gravitino");
      return new BasicAudit(creator, createTime, modifier, now);
    }

    @Override
    public String creator() {
      return creator;
    }

    @Override
    public Instant createTime() {
      return createTime;
    }

    @Override
    public String lastModifier() {
      return lastModifier;
    }

    @Override
    public Instant lastModifiedTime() {
      return lastModifiedTime;
    }
  }
}
