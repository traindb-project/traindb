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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MQueryLog;
import traindb.catalog.pm.MSchema;
import traindb.catalog.pm.MSynopsis;
import traindb.catalog.pm.MTable;
import traindb.catalog.pm.MTask;

public interface CatalogContext {

  /* Modeltype */
  boolean modeltypeExists(String name);

  MModeltype getModeltype(String name);

  Collection<MModeltype> getModeltypes() throws CatalogException;

  Collection<MModeltype> getModeltypes(Map<String, Object> filterPatterns) throws CatalogException;

  MModeltype createModeltype(String name, String type, String location, String className,
                             String uri, String hyperparameters) throws CatalogException;

  void dropModeltype(String name) throws CatalogException;

  /* Model */
  MModel trainModel(
      String modeltypeName, String modelName, String schemaName, String tableName,
      List<String> columnNames, RelDataType dataType, Long baseTableRows, Long trainedRows,
      String options) throws CatalogException;

  void dropModel(String name) throws CatalogException;

  Collection<MModel> getModels() throws CatalogException;

  Collection<MModel> getModels(Map<String, Object> filterPatterns) throws CatalogException;

  Collection<MModel> getInferenceModels(String baseSchema, String baseTable)
      throws CatalogException;

  boolean modelExists(String name);

  MModel getModel(String name);

  /* Synopsis */
  MSynopsis createSynopsis(String synopsisName, String modelName, Integer rows, Double ratio)
      throws CatalogException;

  Collection<MSynopsis> getAllSynopses() throws CatalogException;

  Collection<MSynopsis> getAllSynopses(String baseSchema, String baseTable) throws CatalogException;

  Collection<MSynopsis> getAllSynopses(Map<String, Object> filterPatterns) throws CatalogException;

  boolean synopsisExists(String name);

  MSynopsis getSynopsis(String name);

  void dropSynopsis(String name) throws CatalogException;

  /* Schema & Table */
  MSchema getSchema(String name);

  MTable getTable(String name);

  /* Querylog */
  Collection<MQueryLog> getQueryLogs() throws CatalogException;

  Collection<MQueryLog> getQueryLogs(Map<String, Object> filterPatterns) throws CatalogException;

  MQueryLog insertQueryLog(String start, String user, String query) throws CatalogException;

  void deleteQueryLogs(Integer cnt) throws CatalogException;

  /* Task */
  Collection<MTask> getTaskLogs() throws CatalogException;

  Collection<MTask> getTaskLogs(Map<String, Object> filterPatterns) throws CatalogException;

  MTask insertTask(String time, Integer idx, String task, String status) throws CatalogException;

  void deleteTasks(Integer cnt) throws CatalogException;

  /* Common */
  void close();
}
