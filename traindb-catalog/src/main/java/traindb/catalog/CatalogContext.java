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
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModelInstance;
import traindb.catalog.pm.MSynopsis;

public interface CatalogContext {

  /* Model */
  boolean modelExists(String name) throws CatalogException;
  MModel getModel(String name) throws CatalogException;
  Collection<MModel> getModels() throws CatalogException;
  MModel createModel(String name, String type, String location, String className, String uri)
      throws CatalogException;
  void dropModel(String name) throws CatalogException;
  MModelInstance trainModelInstance(
      String modelName, String modelInstanceName, String schemaName, String tableName,
      List<String> columnNames) throws CatalogException;
  void dropModelInstance(String name) throws CatalogException;
  Collection<MModelInstance> getModelInstances(String modelName) throws CatalogException;
  boolean modelInstanceExists(String name) throws CatalogException;
  MModelInstance getModelInstance(String name) throws CatalogException;
  Path getModelInstancePath(String modelName, String modelInstanceName);
  MSynopsis createSynopsis(String synopsisName, String modeInstanceName) throws CatalogException;
  Collection<MSynopsis> getSynopses() throws CatalogException;

  void close();
}
