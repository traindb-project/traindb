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
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MSynopsis;

public interface CatalogContext {

  /* Model */
  boolean modeltypeExists(String name);

  MModeltype getModeltype(String name);

  Collection<MModeltype> getModeltypes() throws CatalogException;

  MModeltype createModeltype(String name, String type, String location, String className,
                             String uri) throws CatalogException;

  void dropModeltype(String name) throws CatalogException;

  MModel trainModel(
      String modeltypeName, String modelName, String schemaName, String tableName,
      List<String> columnNames, Long baseTableRows, Long trainedRows, String options)
      throws CatalogException;

  void dropModel(String name) throws CatalogException;

  Collection<MModel> getModels() throws CatalogException;

  boolean modelExists(String name);

  MModel getModel(String name);

  Path getModelPath(String modeltypeName, String modelName);

  MSynopsis createSynopsis(String synopsisName, String modelName, Integer rows, Double ratio)
      throws CatalogException;

  Collection<MSynopsis> getAllSynopses() throws CatalogException;

  Collection<MSynopsis> getAllSynopses(String baseSchema, String baseTable) throws CatalogException;

  boolean synopsisExists(String name);

  MSynopsis getSynopsis(String name);

  void dropSynopsis(String name) throws CatalogException;

  void close();
}
