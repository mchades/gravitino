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
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.EntityInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;

/**
 * Client interface for supporting catalogs. It includes methods for listing, loading, creating,
 * altering and dropping catalogs.
 */
@Evolving
public interface SupportsCatalogs {

  /**
   * List the name of all catalogs in the metalake.
   *
   * @return The list of catalog's names.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listCatalogs() throws NoSuchMetalakeException;

  /**
   * List all catalogs with their information in the metalake.
   *
   * @return The list of catalog's information.
   * @throws NoSuchMetalakeException If the metalake with namespace does not exist.
   */
  Catalog[] listCatalogsInfo() throws NoSuchMetalakeException;

  /**
   * Load a catalog by its identifier.
   *
   * @param catalogName the identifier of the catalog.
   * @return The catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  Catalog loadCatalog(String catalogName) throws NoSuchCatalogException;

  /**
   * Check if a catalog exists.
   *
   * @param catalogName The identifier of the catalog.
   * @return True if the catalog exists, false otherwise.
   */
  default boolean catalogExists(String catalogName) {
    try {
      loadCatalog(catalogName);
      return true;
    } catch (NoSuchCatalogException e) {
      return false;
    }
  }

  /**
   * Create a catalog with specified identifier.
   *
   * <p>The parameter "provider" is a short name of the catalog, used to tell Gravitino which
   * catalog should be created. The short name should be the same as the {@link CatalogProvider}
   * interface provided.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   * @throws CatalogAlreadyExistsException If the catalog already exists.
   */
  Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException;

  /**
   * Alter a catalog with specified identifier.
   *
   * @param catalogName the identifier of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the catalog.
   */
  Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException;

  /**
   * Drop a catalog with specified identifier. Please make sure:
   *
   * <ul>
   *   <li>There is no schema in the catalog. Otherwise, a {@link NonEmptyEntityException} will be
   *       thrown.
   *   <li>The method {@link #inactivateCatalog(NameIdentifier)} has been called before dropping the
   *       catalog. Otherwise, a {@link EntityInUseException} will be thrown.
   * </ul>
   *
   * It is equivalent to calling {@code dropCatalog(ident, false)}.
   *
   * @param ident the identifier of the catalog.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty.
   * @throws EntityInUseException If the catalog is in use.
   */
  boolean dropCatalog(NameIdentifier ident) throws NonEmptyEntityException, EntityInUseException;

  /**
   * Drop a catalog with specified identifier. If the force flag is true, it will:
   *
   * <ul>
   *   <li>Cascade drop all sub-entities (schemas, tables, etc.) of the catalog in Gravitino store.
   *   <li>Drop the catalog even if it is in use.
   *   <li>External resources (e.g. database, table, etc.) associated with sub-entities will not be
   *       dropped unless it is managed (such as managed fileset).
   * </ul>
   *
   * @param ident The identifier of the catalog.
   * @param force Whether to force the drop.
   * @return True if the catalog was dropped, false if the catalog does not exist.
   * @throws NonEmptyEntityException If the catalog is not empty and force is false.
   * @throws EntityInUseException If the catalog is in use and force is false.
   */
  boolean dropCatalog(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, EntityInUseException;

  /**
   * Activate a catalog. If the catalog is already active, this method does nothing.
   *
   * @param ident The identifier of the catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  void activateCatalog(NameIdentifier ident) throws NoSuchCatalogException;

  /**
   * Inactivate a catalog. If the catalog is already inactive, this method does nothing. Once a
   * catalog is inactivated:
   *
   * <ul>
   *   <li>It can only be listed, loaded, dropped, or activated.
   *   <li>Any other operations on the catalog will throw an {@link
   *       org.apache.gravitino.exceptions.NonInUseEntityException}.
   *   <li>Any operation on the sub-entities (schemas, tables, etc.) will throw an {@link
   *       org.apache.gravitino.exceptions.NonInUseEntityException}.
   * </ul>
   *
   * @param ident The identifier of the catalog.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  void inactivateCatalog(NameIdentifier ident) throws NoSuchCatalogException;

  /**
   * Test whether the catalog with specified parameters can be connected to before creating it.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if the test failed.
   */
  void testConnection(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception;
}
