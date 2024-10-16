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
package org.apache.gravitino;

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;

/**
 * Client interface for supporting metalakes. It includes methods for listing, loading, creating,
 * altering and dropping metalakes.
 */
@Evolving
public interface SupportsMetalakes {

  /**
   * List all metalakes.
   *
   * @return The list of metalakes.
   */
  Metalake[] listMetalakes();

  /**
   * Load a metalake by its name.
   *
   * @param name the name of the metalake.
   * @return The metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Metalake loadMetalake(String name) throws NoSuchMetalakeException;

  /**
   * Check if a metalake exists.
   *
   * @param name The name of the metalake.
   * @return True if the metalake exists, false otherwise.
   */
  default boolean metalakeExists(String name) {
    try {
      loadMetalake(name);
      return true;
    } catch (NoSuchMetalakeException e) {
      return false;
    }
  }

  /**
   * Create a metalake with specified name, comment and properties.
   *
   * @param name The name of the metalake.
   * @param comment The comment of the metalake.
   * @param properties The properties of the metalake.
   * @return The created metalake.
   * @throws MetalakeAlreadyExistsException If the metalake already exists.
   */
  Metalake createMetalake(String name, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException;

  /**
   * Alter a metalake with specified metalake name and changes.
   *
   * @param name The name of the metalake.
   * @param changes The changes to apply.
   * @return The altered metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the metalake.
   */
  Metalake alterMetalake(String name, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException;

  /**
   * Drop a metalake with specified name. Please make sure:
   *
   * <ul>
   *   <li>There is no catalog in the metalake. Otherwise, a {@link NonEmptyEntityException} will be
   *       thrown.
   *   <li>The method {@link #disableMetalake(String)} has been called before dropping the metalake.
   *       Otherwise, a {@link MetalakeInUseException} will be thrown.
   * </ul>
   *
   * It is equivalent to calling {@code dropMetalake(ident, false)}.
   *
   * @param name The name of the metalake.
   * @return True if the metalake was dropped, false if the metalake does not exist.
   * @throws NonEmptyEntityException If the metalake is not empty.
   * @throws MetalakeInUseException If the metalake is in use.
   */
  default boolean dropMetalake(String name) throws NonEmptyEntityException, MetalakeInUseException {
    return dropMetalake(name, false);
  }

  /**
   * Drop a metalake with specified name. If the force flag is true, it will:
   *
   * <ul>
   *   <li>Cascade drop all sub-entities (tags, catalogs, schemas, tables, etc.) of the metalake in
   *       Gravitino store.
   *   <li>Drop the metalake even if it is in use.
   *   <li>External resources (e.g. database, table, etc.) associated with sub-entities will not be
   *       deleted unless it is managed (such as managed fileset).
   * </ul>
   *
   * If the force flag is false, it is equivalent to calling {@link #dropMetalake(String)}.
   *
   * @param name The name of the metalake.
   * @param force Whether to force the drop.
   * @return True if the metalake was dropped, false if the metalake does not exist.
   * @throws NonEmptyEntityException If the metalake is not empty and force is false.
   * @throws MetalakeInUseException If the metalake is in use and force is false.
   */
  boolean dropMetalake(String name, boolean force)
      throws NonEmptyEntityException, MetalakeInUseException;

  /**
   * Enable a metalake. If the metalake is already in use, this method does nothing.
   *
   * @param name The name of the metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  void enableMetalake(String name) throws NoSuchMetalakeException;

  /**
   * Disable a metalake. If the metalake is already disabled, this method does nothing. Once a
   * metalake is disable:
   *
   * <ul>
   *   <li>It can only be listed, loaded, dropped, or enable.
   *   <li>Any other operations on the metalake will throw an {@link MetalakeNotInUseException}.
   *   <li>Any operation on the sub-entities (catalogs, schemas, tables, etc.) will throw an {@link
   *       MetalakeNotInUseException}.
   * </ul>
   *
   * @param name The name of the metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  void disableMetalake(String name) throws NoSuchMetalakeException;
}
