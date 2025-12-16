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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;

/**
 * A temporary placeholder dispatcher for function operations.
 *
 * <p>The concrete implementation will be wired in a follow-up change; for now we surface the APIs
 * so that REST wiring can be built.
 */
public class UnsupportedFunctionDispatcher implements FunctionDispatcher {

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Function dispatcher is not implemented");
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    throw unsupported();
  }

  @Override
  public Function[] getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    throw unsupported();
  }

  @Override
  public Function[] getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    throw unsupported();
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      FunctionParam[] functionParams,
      Type returnType,
      FunctionImpl[] functionImpls)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    throw unsupported();
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionParam[] functionParams,
      FunctionColumn[] returnColumns,
      FunctionImpl[] functionImpls)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    throw unsupported();
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    throw unsupported();
  }

  @Override
  public boolean deleteFunction(NameIdentifier ident) {
    throw unsupported();
  }

  @Override
  public boolean deleteFunction(NameIdentifier ident, FunctionSignature signature) {
    throw unsupported();
  }
}
