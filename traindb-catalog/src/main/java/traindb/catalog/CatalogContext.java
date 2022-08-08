/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.catalog;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MModelInstance;
import traindb.catalog.pm.MSynopsis;

public interface CatalogContext {

  /* Model */
  boolean modeltypeExists(String name) throws CatalogException;

  MModeltype getModeltype(String name) throws CatalogException;

  Collection<MModeltype> getModeltypes() throws CatalogException;

  MModeltype createModeltype(String name, String type, String location, String className,
                             String uri) throws CatalogException;

  void dropModeltype(String name) throws CatalogException;

  MModelInstance trainModelInstance(
      String modeltypeName, String modelInstanceName, String schemaName, String tableName,
      List<String> columnNames, Long baseTableRows, Long trainedRows) throws CatalogException;

  void dropModelInstance(String name) throws CatalogException;

  Collection<MModelInstance> getModelInstances() throws CatalogException;

  boolean modelInstanceExists(String name) throws CatalogException;

  MModelInstance getModelInstance(String name) throws CatalogException;

  Path getModelInstancePath(String modeltypeName, String modelInstanceName);

  MSynopsis createSynopsis(String synopsisName, String modeInstanceName, Integer rows, Double ratio)
      throws CatalogException;

  Collection<MSynopsis> getAllSynopses() throws CatalogException;

  Collection<MSynopsis> getAllSynopses(String baseSchema, String baseTable) throws CatalogException;

  boolean synopsisExists(String name) throws CatalogException;

  MSynopsis getSynopsis(String name) throws CatalogException;

  void dropSynopsis(String name) throws CatalogException;

  void close();
}
