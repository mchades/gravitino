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

import java.util.Collections;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestManagedFunctionOperations {

  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "schema");
  private static final NameIdentifier IDENT = NameIdentifier.of(NAMESPACE, "doubleup");

  private ManagedFunctionOperations operations;

  @BeforeEach
  public void setUp() {
    EntityStore store = new TestMemoryEntityStore.InMemoryEntityStore();
    IdGenerator idGenerator = new RandomIdGenerator();
    operations = new ManagedFunctionOperations(store, idGenerator);
  }

  @Test
  public void testRegisterAndGetScalarFunction() throws Exception {
    Function function = registerScalar();
    Assertions.assertEquals("doubleup", function.name());
    Assertions.assertEquals(FunctionType.SCALAR, function.functionType());
    Assertions.assertEquals(0, function.version());

    Function loaded = operations.getFunction(IDENT);
    Assertions.assertEquals(function.name(), loaded.name());
    Assertions.assertEquals(function.version(), loaded.version());
  }

  @Test
  public void testRegisterTableFunction() throws Exception {
    FunctionDefinition[] definitions = definitions();
    Function function =
        operations.registerFunction(
            IDENT,
            "table function",
            true,
            new FunctionColumn[] {FunctionColumn.of("col", Types.IntegerType.get(), null)},
            definitions);

    Assertions.assertEquals(FunctionType.TABLE, function.functionType());
    Assertions.assertEquals(1, function.returnColumns().length);
  }

  @Test
  public void testAlreadyExists() throws Exception {
    registerScalar();
    Assertions.assertThrows(
        FunctionAlreadyExistsException.class,
        () ->
            operations.registerFunction(
                IDENT, "dup", FunctionType.SCALAR, true, Types.IntegerType.get(), definitions()));
  }

  @Test
  public void testGetFunctionVersion() throws Exception {
    registerScalar();
    FunctionChange update = FunctionChange.updateComment("new comment");
    Function v1 = operations.alterFunction(IDENT, update);
    Assertions.assertEquals(1, v1.version());
    Assertions.assertEquals("new comment", v1.comment());

    Function version0 = operations.getFunction(IDENT, 0);
    Assertions.assertEquals(0, version0.version());
    Assertions.assertEquals("scalar function", version0.comment());

    Assertions.assertThrows(
        NoSuchFunctionVersionException.class, () -> operations.getFunction(IDENT, 5));
  }

  @Test
  public void testAlterAddDefinition() throws Exception {
    registerScalar();
    FunctionChange addDefinition =
        FunctionChange.addDefinition(
            FunctionDefinitions.of(
                new FunctionParam[] {FunctionParams.of("y", Types.LongType.get(), "long")},
                new FunctionImpl[] {
                  FunctionImpl.ofSql(
                      FunctionImpl.RuntimeType.TRINO, "RETURN y", null, Collections.emptyMap())
                }));

    Function updated = operations.alterFunction(IDENT, addDefinition);
    Assertions.assertEquals(1, updated.version());
    Assertions.assertEquals(2, updated.definitions().length);
  }

  @Test
  public void testAlterRemoveDefinition() throws Exception {
    registerScalar();
    FunctionChange updateComment = FunctionChange.updateComment("keep");
    FunctionChange addDefinition =
        FunctionChange.addDefinition(
            FunctionDefinitions.of(
                new FunctionParam[] {FunctionParams.of("y", Types.LongType.get(), "long")},
                new FunctionImpl[] {
                  FunctionImpl.ofSql(
                      FunctionImpl.RuntimeType.TRINO, "RETURN y", null, Collections.emptyMap())
                }));
    operations.alterFunction(IDENT, updateComment, addDefinition);

    FunctionChange removeDefinition =
        FunctionChange.removeDefinition(
            new FunctionParam[] {FunctionParams.of("y", Types.LongType.get(), "long")});

    Function updated = operations.alterFunction(IDENT, removeDefinition);
    Assertions.assertEquals(2, updated.version());
    Assertions.assertEquals(1, updated.definitions().length);
  }

  @Test
  public void testDropFunction() throws Exception {
    registerScalar();
    Assertions.assertTrue(operations.dropFunction(IDENT));
    Assertions.assertThrows(NoSuchFunctionException.class, () -> operations.getFunction(IDENT));
    Assertions.assertFalse(operations.dropFunction(IDENT));
  }

  private FunctionDefinition[] definitions() {
    return new FunctionDefinition[] {
      FunctionDefinitions.of(
          new FunctionParam[] {FunctionParams.of("x", Types.IntegerType.get(), "input")},
          new FunctionImpl[] {
            FunctionImpl.ofSql(
                FunctionImpl.RuntimeType.TRINO, "RETURN x", null, Collections.emptyMap())
          })
    };
  }

  private Function registerScalar() throws Exception {
    return operations.registerFunction(
        IDENT,
        "scalar function",
        FunctionType.SCALAR,
        true,
        Types.IntegerType.get(),
        definitions());
  }
}
